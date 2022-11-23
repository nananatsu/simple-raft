package parser

import (
	"testing"
)

func TestSelect(t *testing.T) {
	Parse("SELECT * FROM device WHERE device_id=1 AND product_name = 'pname' LIMIT 10,10;")
}

func TestInsert(t *testing.T) {
	Parse("INSERT INTO device ('device_id' , 'device_name' ) VALUES ('1','2'),('3','4');")

}
