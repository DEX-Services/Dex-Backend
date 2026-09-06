package repo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5"
)

const p2pMaxProofBytes = 5 * 1024 * 1024

func (r *P2PRepo) Order(ctx context.Context, userID, orderID string) (*models.P2POrder, error) {
	if err := r.ExpirePendingOrders(ctx, 50); err != nil {
		return nil, err
	}
	o, err := scanOrder(r.pool.QueryRow(ctx, orderSelect+` WHERE id=$1 AND (buyer_id=$2 OR seller_id=$2)`, orderID, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrP2POrderNotFound
	}
	return o, err
}

func (r *P2PRepo) PaymentAccounts(ctx context.Context, userID string) ([]models.P2PPaymentAccount, error) {
	rows, err := r.pool.Query(ctx, `SELECT id::text,method,account_name,account_identifier,instructions,created_at,updated_at FROM p2p_payment_accounts WHERE user_id=$1 ORDER BY method`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2PPaymentAccount{}
	for rows.Next() {
		var account models.P2PPaymentAccount
		if err = rows.Scan(&account.ID, &account.Method, &account.AccountName, &account.AccountIdentifier, &account.Instructions, &account.CreatedAt, &account.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, account)
	}
	return out, rows.Err()
}

func (r *P2PRepo) UpsertPaymentAccount(ctx context.Context, userID, method, name, identifier, instructions string) (*models.P2PPaymentAccount, error) {
	methods, err := normalizePaymentMethods([]string{method})
	if err != nil {
		return nil, err
	}
	name, identifier, instructions = strings.TrimSpace(name), strings.TrimSpace(identifier), strings.TrimSpace(instructions)
	if len(name) < 2 || len(name) > 100 {
		return nil, fmt.Errorf("account name must be 2-100 characters")
	}
	if len(identifier) < 2 || len(identifier) > 200 {
		return nil, fmt.Errorf("payment identifier must be 2-200 characters")
	}
	if len(instructions) > 500 {
		return nil, fmt.Errorf("payment instructions must be at most 500 characters")
	}
	var account models.P2PPaymentAccount
	err = r.pool.QueryRow(ctx, `INSERT INTO p2p_payment_accounts(user_id,method,account_name,account_identifier,instructions) VALUES($1,$2,$3,$4,$5) ON CONFLICT(user_id,method) DO UPDATE SET account_name=EXCLUDED.account_name,account_identifier=EXCLUDED.account_identifier,instructions=EXCLUDED.instructions,updated_at=now() RETURNING id::text,method,account_name,account_identifier,instructions,created_at,updated_at`, userID, methods[0], name, identifier, instructions).Scan(&account.ID, &account.Method, &account.AccountName, &account.AccountIdentifier, &account.Instructions, &account.CreatedAt, &account.UpdatedAt)
	return &account, err
}

func (r *P2PRepo) isOrderParticipant(ctx context.Context, orderID, userID string) (bool, error) {
	var allowed bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM p2p_orders WHERE id=$1 AND (buyer_id=$2 OR seller_id=$2))`, orderID, userID).Scan(&allowed)
	return allowed, err
}

func (r *P2PRepo) OrderMessages(ctx context.Context, userID, orderID string) ([]models.P2POrderMessage, error) {
	allowed, err := r.isOrderParticipant(ctx, orderID, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrP2PForbidden
	}
	rows, err := r.pool.Query(ctx, `SELECT m.id::text,COALESCE(m.sender_id,''),CASE WHEN m.is_system THEN 'System' ELSE COALESCE(u.p2p_username,'User') END,m.body,m.is_system,m.created_at FROM p2p_order_messages m LEFT JOIN users u ON u.id=m.sender_id WHERE m.order_id=$1 ORDER BY m.created_at,m.id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrderMessage{}
	for rows.Next() {
		var item models.P2POrderMessage
		if err = rows.Scan(&item.ID, &item.SenderID, &item.SenderUsername, &item.Body, &item.System, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (r *P2PRepo) AddOrderMessage(ctx context.Context, userID, orderID, body string) (*models.P2POrderMessage, error) {
	body = strings.TrimSpace(body)
	if len(body) == 0 || len(body) > 1000 {
		return nil, fmt.Errorf("message must be 1-1000 characters")
	}
	allowed, err := r.isOrderParticipant(ctx, orderID, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrP2PForbidden
	}
	var item models.P2POrderMessage
	err = r.pool.QueryRow(ctx, `WITH inserted AS (INSERT INTO p2p_order_messages(order_id,sender_id,body) VALUES($1,$2,$3) RETURNING id,sender_id,body,is_system,created_at) SELECT i.id::text,i.sender_id,COALESCE(u.p2p_username,'User'),i.body,i.is_system,i.created_at FROM inserted i JOIN users u ON u.id=i.sender_id`, orderID, userID, body).Scan(&item.ID, &item.SenderID, &item.SenderUsername, &item.Body, &item.System, &item.CreatedAt)
	return &item, err
}

func (r *P2PRepo) AddOrderProof(ctx context.Context, userID, orderID, fileName, mimeType string, data []byte) (*models.P2POrderProof, error) {
	if len(data) == 0 || len(data) > p2pMaxProofBytes {
		return nil, fmt.Errorf("payment proof must be between 1 byte and 5 MB")
	}
	allowedTypes := map[string]bool{"image/jpeg": true, "image/png": true, "image/webp": true, "application/pdf": true}
	if !allowedTypes[mimeType] {
		return nil, fmt.Errorf("payment proof must be JPEG, PNG, WebP, or PDF")
	}
	fileName = strings.TrimSpace(fileName)
	if fileName == "" || len(fileName) > 255 {
		return nil, fmt.Errorf("invalid proof filename")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var buyerID, status string
	if err = tx.QueryRow(ctx, `SELECT buyer_id,status FROM p2p_orders WHERE id=$1 FOR UPDATE`, orderID).Scan(&buyerID, &status); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrP2POrderNotFound
	} else if err != nil {
		return nil, err
	}
	if buyerID != userID {
		return nil, ErrP2PForbidden
	}
	if status != P2PStatusPendingPayment && status != P2PStatusPaymentMade {
		return nil, ErrP2PInvalidState
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM p2p_order_proofs WHERE order_id=$1`, orderID).Scan(&count); err != nil {
		return nil, err
	}
	if count >= 3 {
		return nil, fmt.Errorf("up to 3 payment proofs are allowed")
	}
	var proof models.P2POrderProof
	if err = tx.QueryRow(ctx, `INSERT INTO p2p_order_proofs(order_id,uploader_id,file_name,mime_type,file_data,size_bytes) VALUES($1,$2,$3,$4,$5,$6) RETURNING id::text,file_name,mime_type,size_bytes,created_at`, orderID, userID, fileName, mimeType, data, len(data)).Scan(&proof.ID, &proof.FileName, &proof.MimeType, &proof.SizeBytes, &proof.CreatedAt); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_order_events(order_id,actor_id,kind,metadata) VALUES($1,$2,'payment_proof_uploaded',jsonb_build_object('proofId',$3::text))`, orderID, userID, proof.ID); err != nil {
		return nil, err
	}
	return &proof, tx.Commit(ctx)
}

func (r *P2PRepo) OrderProofs(ctx context.Context, userID, orderID string) ([]models.P2POrderProof, error) {
	allowed, err := r.isOrderParticipant(ctx, orderID, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrP2PForbidden
	}
	rows, err := r.pool.Query(ctx, `SELECT id::text,file_name,mime_type,size_bytes,created_at FROM p2p_order_proofs WHERE order_id=$1 ORDER BY created_at`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrderProof{}
	for rows.Next() {
		var p models.P2POrderProof
		if err = rows.Scan(&p.ID, &p.FileName, &p.MimeType, &p.SizeBytes, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *P2PRepo) OrderProofFile(ctx context.Context, userID, proofID string) (*models.P2POrderProofFile, error) {
	var file models.P2POrderProofFile
	err := r.pool.QueryRow(ctx, `SELECT p.id::text,p.file_name,p.mime_type,p.size_bytes,p.created_at,p.file_data FROM p2p_order_proofs p JOIN p2p_orders o ON o.id=p.order_id WHERE p.id=$1 AND (o.buyer_id=$2 OR o.seller_id=$2)`, proofID, userID).Scan(&file.ID, &file.FileName, &file.MimeType, &file.SizeBytes, &file.CreatedAt, &file.Data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrP2PNotFound
	}
	return &file, err
}

func (r *P2PRepo) OrderEvents(ctx context.Context, userID, orderID string) ([]models.P2POrderEvent, error) {
	allowed, err := r.isOrderParticipant(ctx, orderID, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrP2PForbidden
	}
	rows, err := r.pool.Query(ctx, `SELECT id::text,COALESCE(actor_id,''),kind,metadata,created_at FROM p2p_order_events WHERE order_id=$1 ORDER BY created_at,id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrderEvent{}
	for rows.Next() {
		var e models.P2POrderEvent
		if err = rows.Scan(&e.ID, &e.ActorID, &e.Kind, &e.Metadata, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *P2PRepo) AppealOrder(ctx context.Context, userID, orderID, reason string) (*models.P2POrder, error) {
	reason = strings.TrimSpace(reason)
	if len(reason) < 3 || len(reason) > 500 {
		return nil, fmt.Errorf("appeal reason must be 3-500 characters")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if o.BuyerID != userID && o.SellerID != userID {
		return nil, ErrP2PForbidden
	}
	if o.Status != P2PStatusPaymentMade {
		return nil, ErrP2PInvalidState
	}
	if o.AppealAvailableAt != nil && time.Now().Before(*o.AppealAvailableAt) {
		return nil, fmt.Errorf("appeal is available after %s", o.AppealAvailableAt.Format(time.RFC3339))
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_orders SET status='appeal',appealed_by=$2,appeal_reason=$3,appealed_at=now(),updated_at=now() WHERE id=$1`, orderID, userID, reason); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_order_events(order_id,actor_id,kind,metadata) VALUES($1,$2,'appeal_opened',jsonb_build_object('reason',$3::text))`, orderID, userID, reason); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_order_messages(order_id,body,is_system) VALUES($1,'An appeal was opened. Escrow remains locked for admin review.',true)`, orderID); err != nil {
		return nil, err
	}
	o, err = scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1`, orderID))
	if err != nil {
		return nil, err
	}
	return o, tx.Commit(ctx)
}

func (r *P2PRepo) CancelAppeal(ctx context.Context, userID, orderID string) (*models.P2POrder, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if o.Status != "appeal" || o.AppealedBy != userID {
		return nil, ErrP2PForbidden
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_orders SET status='payment_made',appealed_by=NULL,appeal_reason=NULL,appealed_at=NULL,updated_at=now() WHERE id=$1`, orderID); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_order_events(order_id,actor_id,kind) VALUES($1,$2,'appeal_cancelled')`, orderID, userID); err != nil {
		return nil, err
	}
	o, err = scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1`, orderID))
	if err != nil {
		return nil, err
	}
	return o, tx.Commit(ctx)
}

func (r *P2PRepo) AppealedOrders(ctx context.Context) ([]models.P2POrder, error) {
	rows, err := r.pool.Query(ctx, orderSelect+` WHERE status='appeal' ORDER BY appealed_at,created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrder{}
	for rows.Next() {
		o, scanErr := scanOrder(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

func (r *P2PRepo) AdminOrderProofs(ctx context.Context, orderID string) ([]models.P2POrderProof, error) {
	rows, err := r.pool.Query(ctx, `SELECT id::text,file_name,mime_type,size_bytes,created_at FROM p2p_order_proofs WHERE order_id=$1 ORDER BY created_at`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrderProof{}
	for rows.Next() {
		var p models.P2POrderProof
		if err = rows.Scan(&p.ID, &p.FileName, &p.MimeType, &p.SizeBytes, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *P2PRepo) AdminOrderProofFile(ctx context.Context, proofID string) (*models.P2POrderProofFile, error) {
	var file models.P2POrderProofFile
	err := r.pool.QueryRow(ctx, `SELECT id::text,file_name,mime_type,size_bytes,created_at,file_data FROM p2p_order_proofs WHERE id=$1`, proofID).Scan(&file.ID, &file.FileName, &file.MimeType, &file.SizeBytes, &file.CreatedAt, &file.Data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrP2PNotFound
	}
	return &file, err
}

func (r *P2PRepo) AdminOrderMessages(ctx context.Context, orderID string) ([]models.P2POrderMessage, error) {
	rows, err := r.pool.Query(ctx, `SELECT m.id::text,COALESCE(m.sender_id,''),CASE WHEN m.is_system THEN 'System' ELSE COALESCE(u.p2p_username,'User') END,m.body,m.is_system,m.created_at FROM p2p_order_messages m LEFT JOIN users u ON u.id=m.sender_id WHERE m.order_id=$1 ORDER BY m.created_at,m.id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrderMessage{}
	for rows.Next() {
		var item models.P2POrderMessage
		if err = rows.Scan(&item.ID, &item.SenderID, &item.SenderUsername, &item.Body, &item.System, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (r *P2PRepo) AdminOrderEvents(ctx context.Context, orderID string) ([]models.P2POrderEvent, error) {
	rows, err := r.pool.Query(ctx, `SELECT id::text,COALESCE(actor_id,''),kind,metadata,created_at FROM p2p_order_events WHERE order_id=$1 ORDER BY created_at,id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrderEvent{}
	for rows.Next() {
		var item models.P2POrderEvent
		if err = rows.Scan(&item.ID, &item.ActorID, &item.Kind, &item.Metadata, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
