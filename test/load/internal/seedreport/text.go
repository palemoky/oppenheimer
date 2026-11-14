package seedreport

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

func mib(b int64) string { return fmt.Sprintf("%.1f", float64(b)/(1<<20)) }

// section 写一个标题和一张用制表符分列的表。
func section(w io.Writer, title, header string, rows []string) error {
	fmt.Fprintf(w, "\n== %s ==\n", title)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, header)
	for _, r := range rows {
		fmt.Fprintln(tw, r)
	}
	return tw.Flush()
}

// WriteText 把报告写成人能读的表格。
func (r *Report) WriteText(w io.Writer) error {
	var rows []string
	add := func(format string, args ...any) { rows = append(rows, fmt.Sprintf(format, args...)) }
	flush := func(title, header string) error {
		err := section(w, title, header, rows)
		rows = rows[:0]
		return err
	}

	for _, t := range r.OrderTables {
		add("order_db_%d\t%d\t%s\t%d\t%s\t%s", t.DB, t.Slot, t.Table, t.Rows, mib(t.DataBytes), mib(t.IndexBytes))
	}
	for _, t := range r.IndexTables {
		add("merchant_index_db_%d\t%d\t%s\t%d\t%s\t%s", t.DB, t.Slot, t.Table, t.Rows, mib(t.DataBytes), mib(t.IndexBytes))
	}
	if err := flush("tables (rows exact, sizes from information_schema)", "db\tslot\ttable\trows\tdata MiB\tindex MiB"); err != nil {
		return err
	}

	for _, b := range r.Balance {
		add("%s\t%d\t%d\t%.2f", b.Name, b.Max, b.Min, b.Ratio)
	}
	if err := flush("balance", "what\tmax\tmin\tmax/min"); err != nil {
		return err
	}

	for _, t := range r.HeavyUsers {
		add("user\t%d\t%d\t%d", t.ID, t.Slot, t.Orders)
	}
	for _, t := range r.BigMerchant {
		add("merchant\t%d\t%d\t%d", t.ID, t.Slot, t.Orders)
	}
	if err := flush("heavy users and big merchants", "kind\tid\tslot\torders"); err != nil {
		return err
	}

	if err := writeQueries(w, "query latency", r.Queries, &rows, flush); err != nil {
		return err
	}

	for _, c := range r.Compare {
		add("%s\t%d\t%d\t%.2f / %.2f\t%.2f / %.2f\t%d\t%d\t%t", c.Name, c.Page, c.Offset,
			c.OffsetP50Ms, c.OffsetMaxMs, c.CursorP50Ms, c.CursorMaxMs, c.OffsetRows, c.CursorRows, c.Same)
	}
	if err := flush("offset vs cursor (same deep page)",
		"case\tpage\toffset\toffset p50/max ms\tcursor p50/max ms\trows fetched (offset)\trows fetched (cursor)\tsame page"); err != nil {
		return err
	}
	if r.Archive != nil {
		if err := r.Archive.writeText(w); err != nil {
			return err
		}
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "note: %s\n", n)
	}
	return nil
}

// writeQueries 写一张延迟表和每类查询的执行计划。表格行借用调用方的 rows，flush 把它们写出去。
func writeQueries(w io.Writer, title string, qs []QueryStat, rows *[]string, flush func(title, header string) error) error {
	for _, q := range qs {
		*rows = append(*rows, fmt.Sprintf("%s\t%d\t%.2f\t%.2f\t%.2f\t%.1f", q.Name, q.Runs, q.P50Ms, q.P99Ms, q.MaxMs, q.AvgRows))
	}
	if err := flush(title, "query\truns\tp50 ms\tp99 ms\tmax ms\tavg rows"); err != nil {
		return err
	}
	writeExplain(w, "explain (first run of each query)", qs)
	return nil
}

func writeExplain(w io.Writer, title string, qs []QueryStat) {
	fmt.Fprintf(w, "\n== %s ==\n", title)
	for _, q := range qs {
		fmt.Fprintf(w, "%s\n  %s\n", q.Name, strings.Join(strings.Fields(q.SQL), " "))
		for _, e := range q.Explain {
			fmt.Fprintf(w, "  -> table=%s type=%s key=%s rows=%d filtered=%.0f%% extra=%q", e.Table, e.Type, e.Key, e.Rows, e.Filtered, e.Extra)
			if e.Partitions != "" {
				fmt.Fprintf(w, " partitions=%s", e.Partitions)
			}
			fmt.Fprintln(w)
		}
	}
}

// writeText 写冷库一节。
func (a *ArchiveReport) writeText(w io.Writer) error {
	var rows []string
	flush := func(title, header string) error {
		err := section(w, title, header, rows)
		rows = rows[:0]
		return err
	}
	for _, t := range a.Tables {
		rows = append(rows, fmt.Sprintf("%s\t%d\t%s\t%s\t%.1f", t.Table, t.Rows, mib(t.DataBytes), mib(t.IndexBytes), t.BytesPerRow))
	}
	if err := flush("cold archive: tables (rows exact, sizes summed over partitions)", "table\trows\tdata MiB\tindex MiB\tbytes/row"); err != nil {
		return err
	}
	for _, p := range a.Partitions {
		rows = append(rows, fmt.Sprintf("%s\t%s\t%d\t%s\t%s", p.Table, p.Partition, p.EstRows, mib(p.DataBytes), mib(p.IndexBytes)))
	}
	if err := flush("cold archive: partitions (rows are ESTIMATES)", "table\tpartition\test. rows\tdata MiB\tindex MiB"); err != nil {
		return err
	}
	for _, c := range a.Compression {
		ratio := "n/a"
		if c.Ratio > 0 {
			ratio = fmt.Sprintf("%.2f", c.Ratio)
		}
		rows = append(rows, fmt.Sprintf("%s\t%d\t%.1f\t%d\t%.1f\t%s", c.Table, c.HotRows, c.HotBytesPerRow, c.ColdRows, c.ColdBytesPerRow, ratio))
	}
	if err := flush("cold archive: bytes per row, hot vs cold", "table\thot rows\thot bytes/row\tcold rows\tcold bytes/row\thot/cold"); err != nil {
		return err
	}
	if len(a.Queries) == 0 {
		fmt.Fprintln(w, "\n== cold archive: query latency ==\n(no queries)")
	} else if err := writeQueries(w, "cold archive: query latency", a.Queries, &rows, flush); err != nil {
		return err
	}
	for _, n := range a.Notes {
		fmt.Fprintf(w, "note: %s\n", n)
	}
	return nil
}
