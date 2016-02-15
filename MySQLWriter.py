import mysql.connector
from pybean import SQLiteWriter

__version__ = "0.2.1"
__author__ = "Fabien Di Tore"
__email__ = "fabien@ditore.ch"

class MySQLWriter(SQLiteWriter):

    """
    In frozen mode (the default), the writer will not alter db schema.
    Just add frozen=False to enable column creation (or just add False
    as second parameter):

    query_writer = SQLiteWriter(":memory:", False)
    """
    def __init__(self, frozen=True, **mysqloptions):
        try:
            self.frozen = frozen

            self.db = mysql.connector.connect(**mysqloptions)

            self.cursor = self.db.cursor(dictionary=True, buffered=True)
            self.engine = "INNODB"
        except Exception as e: 
            print "excteption "+str(e)

    def commit(self):
        self.db.commit()

    def replace(self, bean):
        keys = []
        values = []
        write_operation = "replace"
        if "id" not in bean.__dict__:
            write_operation = "insert"
            #keys.append("id")
            #values.append(None)
        self.__create_table(bean.__class__.__name__)
        columns = self.__get_columns(bean.__class__.__name__)
        for key in bean.__dict__:
            keys.append(key)
            if key not in columns:
                self.__create_column(bean.__class__.__name__, key,
                        type(bean.__dict__[key]))
            values.append(bean.__dict__[key])
        sql  = write_operation + " into " + bean.__class__.__name__ + "("
        sql += ",".join(keys) + ") values ("
        sql += ",".join(["%s" for i in keys])  +  ")"
        #print sql,tuple(values )
        self.cursor.execute(sql, tuple(values))
        #print(self.cursor._executed)

        if write_operation == "insert":
            bean.id = self.cursor.lastrowid
        
        return bean.id

    def __create_table(self, table):
        if self.frozen:
            return
        sql = "CREATE TABLE IF NOT EXISTS `"+ table +"`  ( `id` INT NOT NULL AUTO_INCREMENT , PRIMARY KEY (`id`)) ENGINE = InnoDB;"
        self.cursor.execute(sql)

    def __create_column(self, table, column, sqltype):
        if self.frozen:
            return
        if sqltype in [long, complex, float, int, bool]:
            sqltype = "NUMERIC"
        else:
            sqltype = "TEXT"
        sql = "ALTER TABLE `" + table + "` add `" + column + "` " + sqltype + " NULL"
        #ALTER TABLE `test` ADD `test` INT NULL
        self.cursor.execute(sql)

    def __get_columns(self, table):
        columns = []
        if self.frozen:
            return columns
        self.cursor.execute("SHOW COLUMNS FROM `" + table  + "`")
        for row in self.cursor:
            columns.append(row["Field"])
        return columns

    def delete(self, bean):
        self.__create_table(bean.__class__.__name__)
        sql = "delete from " + bean.__class__.__name__ + " where id=%s"
        self.cursor.execute(sql,(bean.id,))

    def get_rows(self, table_name, sql = "1", replace = None):
        if replace is None : replace = []
        self.__create_table(table_name)
        sql = "SELECT * FROM `" + table_name + "` WHERE " + sql
        try:
            self.cursor.execute(sql, tuple(replace))
            for row in self.cursor:
                yield row
        except Exception as e:
            return

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def delete(self, bean):
        self.__create_table(bean.__class__.__name__)
        sql = "delete from `" + bean.__class__.__name__ + "` where id=%s"
        self.cursor.execute(sql,(bean.id,))