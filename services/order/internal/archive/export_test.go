package archive

// SetAfterColdWrite 设置测试钩子：每批写进冷库之后、读回比对之前调用，返回错误时这一批就此中止。
func SetAfterColdWrite(a *Archiver, f func(slot int, ids []int64) error) { a.afterColdWrite = f }
