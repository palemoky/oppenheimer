package store

// SetStatementRows 把一条写入语句的行数上限改成 n，让测试用少量的行覆盖“一组行切成几条语句”的路径。
func SetStatementRows(s *Store, n int) { s.stmtRows = n }
