package store

import "context"

// CountActiveTasksByState returns counts of non-terminal tasks grouped by state
// (uses the partial indexes on the active states — cheap).
func (p *Postgres) CountActiveTasksByState(ctx context.Context) (map[string]int, error) {
	const q = `
		SELECT state::text, count(*) FROM tasks
		WHERE state IN ('queued', 'dispatching', 'running', 'retrying')
		GROUP BY state`
	rows, err := p.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int, 4)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[state] = n
	}
	return out, rows.Err()
}

// CountAliveWorkers returns the number of workers currently marked alive.
func (p *Postgres) CountAliveWorkers(ctx context.Context) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE status = 'alive'`).Scan(&n)
	return n, err
}

// CountPendingOutbox returns the number of unpublished outbox rows.
func (p *Postgres) CountPendingOutbox(ctx context.Context) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}
