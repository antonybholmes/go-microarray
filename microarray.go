package microarray

import (
	"database/sql"

	"github.com/antonybholmes/go-sys"
)

const ALL_SAMPLES_SQL = `SELECT uuid, array, name
	FROM samples ORDER BY array, name`

type MicroarrayDB struct {
	db             *sql.DB
	allSamplesStmt *sql.Stmt
}

func NewMicroarrayDb(file string) (*MicroarrayDB, error) {
	db := sys.Must(sql.Open("sqlite3", file))

	return &MicroarrayDB{db: db,
		allSamplesStmt: sys.Must(db.Prepare(ALL_SAMPLES_SQL)),
	}, nil
}

func (microarraydb *MicroarrayDB) Close() {
	microarraydb.db.Close()
}
