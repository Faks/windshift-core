package repository

import (
	"fmt"
	"time"
)

// DueSoonItem is the projection the due-date reminder sweep needs: the
// opted-in recipient to notify, their configured lead time, and enough item
// identity to build a deep link and delivery key.
type DueSoonItem struct {
	ItemID       int
	WorkspaceID  int
	WorkspaceKey string
	ItemNumber   int
	Title        string
	DueDate      time.Time
	RecipientID  int
	// ReminderDays is the recipient's configured lead time in days.
	ReminderDays int
}

// ClaimDueDateReminder records that a due-date reminder was sent for the
// (item, user, due date) triple and reports whether this call created the
// record. A false result means a reminder was already recorded, so dismissing
// the notification cannot resurrect it on the next sweep.
func (r *ItemRepository) ClaimDueDateReminder(itemID, userID int, dueDate string) (bool, error) {
	result, err := r.db.ExecWrite(`
		INSERT INTO item_due_reminders (item_id, user_id, due_date)
		VALUES (?, ?, ?)
		ON CONFLICT(item_id, user_id, due_date) DO NOTHING
	`, itemID, userID, dueDate)
	if err != nil {
		return false, fmt.Errorf("claim due-date reminder: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read due-date reminder claim result: %w", err)
	}
	return rows == 1, nil
}

// ListDueSoonItemsForReminderUsers returns up to limit non-completed items due
// inside [from, to] whose recipient has opted into due-date reminders. The
// recipient is the assignee for regular items and the workspace owner for
// personal-workspace items without an assignee. Each row carries the
// recipient's configured lead time so the caller can apply the per-user window.
func (r *ItemRepository) ListDueSoonItemsForReminderUsers(from, to time.Time, limit int) ([]DueSoonItem, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := r.db.Query(`
		SELECT i.id, i.workspace_id, COALESCE(w.key, ''), i.workspace_item_number,
		       i.title, i.due_date, COALESCE(i.assignee_id, w.owner_id), up.due_date_reminder_days
		FROM items i
		JOIN workspaces w ON w.id = i.workspace_id
		JOIN user_preferences up ON up.user_id = COALESCE(i.assignee_id, w.owner_id)
		LEFT JOIN statuses st ON st.id = i.status_id
		LEFT JOIN status_categories sc ON sc.id = st.category_id
		WHERE up.due_date_reminders_enabled = TRUE
		  AND i.due_date IS NOT NULL
		  AND i.due_date >= ?
		  AND i.due_date <= ?
		  AND COALESCE(sc.is_completed, FALSE) = FALSE
		  AND (i.assignee_id IS NOT NULL OR w.is_personal = TRUE)
		ORDER BY i.due_date ASC, i.id ASC
		LIMIT ?
	`, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("list due-soon items for reminder users: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]DueSoonItem, 0, limit)
	for rows.Next() {
		var item DueSoonItem
		if err := rows.Scan(&item.ItemID, &item.WorkspaceID, &item.WorkspaceKey, &item.ItemNumber,
			&item.Title, &item.DueDate, &item.RecipientID, &item.ReminderDays); err != nil {
			return nil, fmt.Errorf("scan due-soon reminder item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due-soon reminder items: %w", err)
	}
	return items, nil
}
