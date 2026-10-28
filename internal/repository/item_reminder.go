package repository

import (
	"fmt"
	"time"
)

// DueSoonItem is the projection the due-date reminder sweep needs: the
// assignee to notify and enough item identity to build a deep link and
// delivery key.
type DueSoonItem struct {
	ItemID       int
	WorkspaceID  int
	WorkspaceKey string
	ItemNumber   int
	Title        string
	DueDate      time.Time
	AssigneeID   int
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

// ListDueSoonOpenItems returns up to limit non-completed items whose due date
// falls inside [from, to] and that have an assignee. Due dates are calendar
// dates, so callers pass date-aligned bounds. Ordered oldest first so a capped
// batch drains the most urgent items.
func (r *ItemRepository) ListDueSoonOpenItems(from, to time.Time, limit int) ([]DueSoonItem, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := r.db.Query(`
		SELECT i.id, i.workspace_id, COALESCE(w.key, ''), i.workspace_item_number,
		       i.title, i.due_date, i.assignee_id
		FROM items i
		JOIN workspaces w ON w.id = i.workspace_id
		LEFT JOIN statuses st ON st.id = i.status_id
		LEFT JOIN status_categories sc ON sc.id = st.category_id
		WHERE i.due_date IS NOT NULL
		  AND i.assignee_id IS NOT NULL
		  AND i.due_date >= ?
		  AND i.due_date <= ?
		  AND COALESCE(sc.is_completed, FALSE) = FALSE
		ORDER BY i.due_date ASC, i.id ASC
		LIMIT ?
	`, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("list due-soon items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]DueSoonItem, 0, limit)
	for rows.Next() {
		var item DueSoonItem
		if err := rows.Scan(&item.ItemID, &item.WorkspaceID, &item.WorkspaceKey, &item.ItemNumber,
			&item.Title, &item.DueDate, &item.AssigneeID); err != nil {
			return nil, fmt.Errorf("scan due-soon item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due-soon items: %w", err)
	}
	return items, nil
}
