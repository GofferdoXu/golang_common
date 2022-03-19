package test

import (
	"context"
	"fmt"
	"github.com/GofferdoXu/golang_common/lib"
	"gorm.io/gorm"
	"testing"
	"time"
)

type Test2 struct {
	Id        int64     `json:"id" gorm:"primary_key"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (f *Test2) Table() string {
	return "test1"
}

func (f *Test2) DB() *gorm.DB {
	return lib.GORMDefaultPool
}

var (
	createPGTableSQL = "CREATE TABLE `test1` (`id` int(12) unsigned NOT NULL AUTO_INCREMENT" +
		" COMMENT '自增id',`name` varchar(255) NOT NULL DEFAULT '' COMMENT '姓名'," +
		"`created_at` datetime NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB " +
		"DEFAULT CHARSET=utf8"
	insertPGSQL    = "INSERT INTO `test1` (`id`, `name`, `created_at`) VALUES (NULL, '111', '2018-08-29 11:01:43');"
	dropTablePGSQL = "DROP TABLE `test1`"
	beginPGSQL     = "start transaction;"
	commitPGSQL    = "commit;"
	rollbackPGSQL  = "rollback;"
)

func Test_PGDBPool(t *testing.T) {
	SetUp()

	//获取链接池
	dbpool, err := lib.GetPGDBPool("default")
	if err != nil {
		t.Fatal(err)
	}
	//开始事务
	trace := lib.NewTrace()
	if _, err := lib.PGDBPoolLogQuery(trace, dbpool, beginPGSQL); err != nil {
		t.Fatal(err)
	}

	//创建表
	if _, err := lib.PGDBPoolLogQuery(trace, dbpool, createPGTableSQL); err != nil {
		lib.PGDBPoolLogQuery(trace, dbpool, rollbackPGSQL)
		t.Fatal(err)
	}

	//插入数据
	if _, err := lib.PGDBPoolLogQuery(trace, dbpool, insertPGSQL); err != nil {
		lib.PGDBPoolLogQuery(trace, dbpool, rollbackPGSQL)
		t.Fatal(err)
	}

	//循环查询数据
	current_id := 0
	table_name := "test1"
	fmt.Println("begin read table ", table_name, "")
	fmt.Println("------------------------------------------------------------------------")
	fmt.Printf("%6s | %6s\n", "id", "created_at")
	for {
		rows, err := lib.PGDBPoolLogQuery(trace, dbpool, "SELECT id,created_at FROM test1 WHERE id>? order by id asc", current_id)
		defer rows.Close()
		row_len := 0
		if err != nil {
			lib.PGDBPoolLogQuery(trace, dbpool, "rollback;")
			t.Fatal(err)
		}
		for rows.Next() {
			var create_time string
			if err := rows.Scan(&current_id, &create_time); err != nil {
				lib.PGDBPoolLogQuery(trace, dbpool, "rollback;")
				t.Fatal(err)
			}
			fmt.Printf("%6d | %6s\n", current_id, create_time)
			row_len++
		}
		if row_len == 0 {
			break
		}
	}
	fmt.Println("------------------------------------------------------------------------")
	fmt.Println("finish read table ", table_name, "")

	//删除表
	if _, err := lib.PGDBPoolLogQuery(trace, dbpool, dropTableSQL); err != nil {
		lib.PGDBPoolLogQuery(trace, dbpool, rollbackPGSQL)
		t.Fatal(err)
	}

	//提交事务
	lib.PGDBPoolLogQuery(trace, dbpool, commitPGSQL)
	TearDown()
}

func Test_PGGORM(t *testing.T) {
	SetUp()

	//获取链接池
	dbpool, err := lib.GetPGGormPool("default")
	if err != nil {
		t.Fatal(err)
	}
	db := dbpool.Begin()
	traceCtx := lib.NewTrace()
	ctx := context.Background()
	ctx = lib.SetTraceContext(ctx, traceCtx)
	//设置trace信息
	db = db.WithContext(ctx)
	if err := db.Exec(createPGTableSQL).Error; err != nil {
		db.Rollback()
		t.Fatal(err)
	}

	//插入数据
	t1 := &Test1{Name: "test_name", CreatedAt: time.Now()}
	if err := db.Save(t1).Error; err != nil {
		db.Rollback()
		t.Fatal(err)
	}

	//查询数据
	list := []Test1{}
	if err := db.Where("name=?", "test_name").Find(&list).Error; err != nil {
		db.Rollback()
		t.Fatal(err)
	}
	fmt.Println(list)

	//删除表数据
	if err := db.Exec(dropTableSQL).Error; err != nil {
		db.Rollback()
		t.Fatal(err)
	}
	db.Commit()
	TearDown()
}
