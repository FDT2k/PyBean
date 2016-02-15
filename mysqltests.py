import unittest

from pybean import Store
from MySQLWriter import MySQLWriter
import datetime

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
        db.cursor.execute("select * from typemap;")
        for row in db.cursor:
            for key in row:
                print row[key].__class__.__name__,key

    def test_new_bean_type(self):
        db = self.get_fluid_save()
        bean = db.new("book")
        self.assertEqual(bean.__class__.__name__, "book")
        bean.title="test"
        bean.total_rent_time = datetime.timedelta(0,6400)
        _id = db.save(bean)
        db.commit()



     #   self.assertEqual(bean.id,1)

     #    for book in db.find("typemap","",[]):

      #     print book.__class__.__name__
     #   db.commit()

if __name__ == '__main__':
    unittest.main()