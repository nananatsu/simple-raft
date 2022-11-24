package sql

import (
	"testing"
)

func TestCreate(t *testing.T) {

	ParseSQL(`CREATE TABLE ` + "`user`" + ` (
		` + "`user_id`" + ` INT NOT NULL,
		` + "`special_role`" + ` VARCHAR DEFAULT NULL,
		` + "`usr_biz_type`" + ` VARCHAR DEFAULT NULL,
		` + "`user_code`" + ` VARCHAR DEFAULT NULL,
		` + "`nickname`" + ` VARCHAR DEFAULT NULL,
		` + "`avatar`" + ` VARCHAR DEFAULT NULL,
		` + "`sex`" + ` INT DEFAULT NULL,
		` + "`division_code`" + ` VARCHAR DEFAULT NULL,
		` + "`detailed_address`" + ` VARCHAR DEFAULT NULL ,
		` + "`is_enabled`" + ` INT NOT NULL DEFAULT '1',
		PRIMARY KEY (` + "`user_id`" + `),
		UNIQUE KEY user_code_UNIQUE (` + "`user_code`" + `)
	  );`)
}

func TestSelect(t *testing.T) {
	ParseSQL("SELECT * FROM device WHERE device_id=1 AND product_name = 'pname \t\\<>12 ' LIMIT 10,10;")
}

func TestInsert(t *testing.T) {
	ParseSQL("INSERT INTO device ('device_id' , 'device_name' ) VALUES ('1~sd\n==dfds','2'),('3','4');")
}
