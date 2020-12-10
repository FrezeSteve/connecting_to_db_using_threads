from constants import SQL_STATEMENTS
from json import dumps
import os
import pandas as pd
import psycopg2
from psycopg2 import pool
import sys
from random import choice
from threading import Thread, Lock
import time
from tqdm import tqdm


class DumpData:
    def __init__(self, _no_customer):
        self.base_file_path = os.path.dirname(os.path.abspath(__file__))
        self.data_path = None
        self.count = 0
        self.threads_list = []
        self.results = []
        connection = None
        cursor_ = None
        self.user = "user of db"
        self.password = "password"
        self.host = "current ip"
        self.port = "port"
        self.database = "db name"
        try:
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.database)
            cursor_ = connection.cursor()
            cursor_.execute("SELECT version();")
            record = cursor_.fetchone()
            print("You are connected to - ", record, "\n")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            raise Exception(error)
        finally:
            # closing database connection.
            if connection:
                cursor_.close()
                connection.close()
        connection = psycopg2.connect(user=self.user,
                                      password=self.password,
                                      host=self.host,
                                      port=self.port,
                                      database=self.database)
        cursor = connection.cursor()
        cursor.execute(SQL_STATEMENTS['customer'])
        self._queryset_customer = cursor.fetchmany(size=_no_customer)
        self._queryset_customer = tuple(self._queryset_customer)
        del cursor
        cursor = connection.cursor()
        cursor.execute(SQL_STATEMENTS['connections'])
        self._max_connection = None
        self._max_connection = cursor.fetchone()
        self._max_connection = int(int(self._max_connection[0]) * 3 / 4)
        cursor.close()
        self.thread_lock = Lock()
        connection.close()
        self.__threaded_connection = pool.ThreadedConnectionPool(
            50, self._max_connection,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database)

    def get_connection(self):
        connecting = True
        connection = None
        count = range(10)
        with self.thread_lock:
            while connecting:
                try:
                    # if self.__threaded_connection.getconn():
                    connection = self.__threaded_connection.getconn()
                    connecting = False
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    time.sleep(choice(count))
            return connection

    @staticmethod
    def get_payment(customer_id_, connection):
        cursor = connection.cursor()
        sql = SQL_STATEMENTS['payment'] + customer_id_
        cursor.execute(sql)
        _queryset_payment = cursor.fetchall()
        cursor.close()
        payment = []
        for i in tuple(_queryset_payment):
            payment.append({
                "customer_id": i[2],
                "staff_id": i[0],
                "rental_id": i[1],
            })
        return payment

    @staticmethod
    def film_section(customer_id_, connection):
        cursor = connection.cursor()
        sql = SQL_STATEMENTS['film_section_1'] + customer_id_ + SQL_STATEMENTS['film_section_2']
        cursor.execute(sql)
        # _queryset_payment = cursor.fetchmany(5)
        _queryset_payment = cursor.fetchall()
        cursor.close()

        data = [
            {
                "title": i[0],
                "description": i[1],
                "rental_duration": i[2],
                "customer_id": i[3],
            }
            for i in tuple(_queryset_payment)
        ]
        # for i in tuple(_queryset_payment):
        #     sql_2 = SQL_STATEMENTS['inventory_section'] + str(i[1])
        #     cursor = connection.cursor()
        #     cursor.execute(sql_2)
        #     _qs = tuple(cursor.fetchmany(5))
        #     data.append({
        #         "title": i[0],
        #         "description": i[1],
        #         "rental_duration": i[2],
        #         "customer_id": i[3],
        #     })
        cursor.close()
        return data

    @staticmethod
    def store_section(address_id_, connection):
        cursor = connection.cursor()
        sql = SQL_STATEMENTS['store_section'] + address_id_
        cursor.execute(sql)
        _queryset_payment = cursor.fetchall()
        cursor.close()
        data = []
        for i in tuple(_queryset_payment):
            data.append({
                "staff_id": i[0],
                "manager_staff_id": i[1]
            })
        return data

    def get_customer_object(self, i_, storage_variable_list, progress_bar_):
        connection = self.get_connection()
        p_value = 10

        progress_bar_.update(p_value)

        customer_object = {}
        customer_id = str(i_[0])
        address_id = str(i_[-1])
        # implement appending to customer object
        customer_object['customer_name'] = i_[2] + " " + i_[1]
        customer_object['address'] = i_[4]
        customer_object['email'] = i_[3]
        customer_object['customer_id'] = customer_id

        progress_bar_.update(p_value)

        customer_object['payment'] = self.get_payment(customer_id, connection)

        progress_bar_.update(p_value)

        customer_object['film_section'] = self.film_section(customer_id, connection)

        progress_bar_.update(p_value)

        customer_object['store_section'] = self.store_section(address_id, connection)

        progress_bar_.update(p_value)

        with Lock():
            progress_bar_.update(p_value * 2)

            storage_variable_list.append(dumps(customer_object))

            progress_bar_.update(p_value * 2)

        del address_id
        del customer_id
        del customer_object
        self.__threaded_connection.putconn(connection)

        progress_bar_.update(p_value)

    def append_thread_to_list(self, function_, *args):
        x = Thread(target=function_, args=args, daemon=True)
        x.start()
        self.threads_list.append(x)

    def join_threads(self):
        [thread_.join() for thread_ in self.threads_list]

    def write_results_to_file(self):
        start_time = time.time()
        ending_point = len(self._queryset_customer)
        with tqdm(total=ending_point * 100, file=sys.stdout) as __p_bar:
            [
                self.append_thread_to_list(self.get_customer_object, customer, self.results, __p_bar)
                for customer in self._queryset_customer
            ]
            [thread_.join() for thread_ in self.threads_list]
        self.__threaded_connection.closeall()
        list_object = dumps(self.results)
        with open('output.json', 'w') as file:
            file.write(list_object)
        del file
        print("--- %.2f seconds ---" % (time.time() - start_time))
        return float('%.2f' % (time.time() - start_time))


test_ = (1, )
for i in test_:
    db_instance = DumpData(i)
    time_taken = db_instance.write_results_to_file()
    with open('output.txt', 'a+') as file:
        file.write(str(time_taken) + '\n')
    del file
# db_instance = DumpData(1)
# time_taken = db_instance.write_results_to_file()
# print(time_taken)
