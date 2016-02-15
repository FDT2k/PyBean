import unittest

from pybean import Store
from MySQLWriter import MySQLWriter

class TestMysql(unittest.TestCase):
    def setUp(self):


        pass

    def tearDown(self):
        #clean up mysql
        """
        print "TearDwn"
        sql ="DROP TABLE book"
        store = self.get_fluid_save()
        db = store.writer
        #print db
        db.cursor.execute(sql)
        db.commit()
        """

    def get_frozen_save(self):
        return Store(MySQLWriter(True,user="test",password="test",database="py",host="localhost"))

    def get_fluid_save(self):
        return Store(MySQLWriter(False,user="test",password="test",database="py",host="localhost"))

    def test_one(self):

        store = self.get_fluid_save()
        db = store.writer
        #print db
        db.cursor.execute("select * from test")
        for row in db.cursor:
            print row

    def test_new_bean_type(self):
        db = self.get_fluid_save()
        bean = db.new("book")
        self.assertEqual(bean.__class__.__name__, "book")
        bean.title="test"
        _id = db.save(bean)
        db.commit()

     #   self.assertEqual(bean.id,1)

        for book in db.find("book","title like %s",["test"]):
            print book.title
        db.commit()

if __name__ == '__main__':
    unittest.main()