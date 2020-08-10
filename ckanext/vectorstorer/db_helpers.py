import psycopg2
import urlparse
class DB:

    def __init__(self,db_conn_params):
        result = urlparse.urlparse(db_conn_params)
        user = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        self.conn = psycopg2.connect(database=database, user=user, password=password,host=hostname)
        self.cursor=self.conn.cursor()

    def check_if_table_exists(self,table_name):
        self.cursor.execute("SELECT * FROM information_schema.tables WHERE table_name='%s'"%table_name)
        table_exists=bool(self.cursor.rowcount)
        return table_exists

    def create_table(self, table_name, fin, geometry, srs, coordinate_dimension):
        field_info = []

        if self.check_if_table_exists(table_name):
            # save the field info if the table exists.
            self.cursor.execute("""select pa.attname as name, pd.description as info
                                   from pg_class pc, pg_attribute pa, pg_description pd
                                   where pa.attrelid = pc.oid and pd.objoid = pc.oid
                                   and pd.objsubid = pa.attnum and pc.relname = %s
                                   and pa.attnum > 0 """,
                                (table_name,))

            field_info = self.cursor.fetchall()
            self.drop_table(table_name)

        self.cursor.execute("CREATE TABLE \"%s\" (_id serial PRIMARY KEY, %s);"%(table_name, fin.decode('utf-8')))
        self.cursor.execute("SELECT AddGeometryColumn ('%s','the_geom',%s,'%s',%s);"%(table_name,
                                                                                      srs,
                                                                                      geometry,
                                                                                      coordinate_dimension))
        # this has to be last because they use unnamed inserts, and that can generally ignore the last column
        self.cursor.execute('''alter table "%s" add column _full_text tsvector''' %(table_name))

        #restore the field info
        comment = """comment on column "%s"."%s" is %%s"""
        for field, info in field_info:
            if field in fin:
                self.cursor.execute(comment % (table_name, field.decode('utf-8')), (info.decode('utf-8'),))



    def insert_to_table(self,table,fields,geometry_text,convert_to_multi,srs):
        if convert_to_multi:
            insert=("INSERT INTO \"%s\" VALUES (%s ST_Multi(ST_GeomFromText('%s',%s)));"%(table,fields.encode('utf-8').decode('utf-8'),geometry_text,srs))
        else:
            insert=("INSERT INTO \"%s\" VALUES (%s ST_GeomFromText('%s',%s));"%(table,fields.encode('utf-8').decode('utf-8'),geometry_text,srs))
        self.cursor.execute(insert)

    def create_spatial_index(self,table):
        indexing=("CREATE INDEX \"%s_the_geom_idx\" ON \"%s\" USING GIST(the_geom);"%(table,table))
        self.cursor.execute(indexing)

    def drop_table(self,table):
        indexing=("DROP TABLE \"%s\";"%(table))
        self.cursor.execute(indexing)

    def commit_and_close(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
