package domain

type ETL interface {
	InsertRawData(file string) error
	InsertCleanData() error
	RecordCount()
}
