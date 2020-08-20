import psycopg2
import urlparse
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

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
        self.cursor.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", table_name)
        table_exists=bool(self.cursor.rowcount)
        if table_exists:
            return True
        else:
            return False

    def create_table(self,table_name,fin,geometry,srs,coordinate_dimension):
        self.cursor.execute("CREATE TABLE \"%s\" (_id serial PRIMARY KEY, %s);"%(table_name, fin))
        self.cursor.execute("SELECT AddGeometryColumn (%s,'the_geom',%s, %s, %s);",
                            (table_name, srs, geometry, coordinate_dimension))

    def join_target_id(self):
        try:
            from ckanext.landesa_tenure import datastore
            resource = datastore.get_resource()
            return resource['id']
        except Exception as msg:
            log.debug("Error getting the resource id: %s" % msg)
            raise Exception("Couldn't find the resource in join_target_id")

    def ckan_mapping_id(self):
        try:
            from ckanext.landesa_tenure import datastore
            resource = datastore.get_mapping_resource()
            return resource['id']
        except Exception as msg:
            log.debug("Error getting the resource id: %s" % msg)
            raise Exception("Couldn't find the resource in ckan_mapping_id")

    def create_table_and_view(self, table_name, fin, geometry, srs, coordinate_dimension):
        view_name = table_name
        table_name = "%s_tbl" % table_name

        join_target_id = self.join_target_id()

        self.cursor.execute("""CREATE TABLE "%s" (_id serial PRIMARY KEY, %s);""" % (table_name,fin.decode('utf-8')))
        self.cursor.execute("SELECT AddGeometryColumn (%s,'the_geom',%s, %s, %s);",
                                    (table_name, srs, geometry, coordinate_dimension))
        self.cursor.execute("""
            CREATE VIEW "%(view_name)s" as
              select "%(table_name)s".*,
                     "%(join_target_id)s".link,
                     "%(join_target_id)s".link_my
              from
                 "%(table_name)s" left outer join "%(join_target_id)s"
                    on ("%(join_target_id)s".parcel_id = "%(table_name)s"."CKAN_ID");
        """ % {'view_name': view_name,
               'table_name': table_name,
               'join_target_id': join_target_id}
        )
        self.cursor.execute("""
            CREATE INDEX "%s_parcel_idx" on "%s"("CKAN_ID")
        """ % (table_name, table_name)
        )

        # UNDONE -- rule for inserts on the table instead of the view
        # updates/deletes

        return table_name;

    def insert_to_table(self,table,fields,geometry_text,convert_to_multi,srs):
        if convert_to_multi:
            insert=("INSERT INTO \"%s\" VALUES (%s ST_Multi(ST_GeomFromText('%s',%s)));"%(table,fields.encode('utf-8').decode('utf-8'),geometry_text,srs))
        else:
            insert=("INSERT INTO \"%s\" VALUES (%s ST_GeomFromText('%s',%s));"%(table,fields.encode('utf-8').decode('utf-8'),geometry_text,srs)) 
        self.cursor.execute(insert)

    def create_ckan_mapping(self, table_name):
        resource_id = self.ckan_mapping_id()
        sql = """insert into "%s" (package_id, parcel_id) select '%s', "CKAN_ID" from "%s" """ % (
            resource_id, table_name.replace("_tbl", ''), table_name)
        self.cursor.execute(sql)

    def create_spatial_index(self,table):
        indexing=("CREATE INDEX \"%s_the_geom_idx\" ON \"%s\" USING GIST(the_geom);"%(table,table)) 
        self.cursor.execute(indexing)

    def drop_table(self,table):
        indexing=("DROP TABLE \"%s\";"%(table))
        self.cursor.execute(indexing)

    def drop_view(self, view):
        indexing=("DROP view \"%s\";"%(view))
        self.cursor.execute(indexing)
        resource_id = self.ckan_mapping_id()
        sql = """ delete from "%s" where package_id='%s' """ % (resource_id, view)
        self.cursor.execute(sql)

    def rollback(self):
        self.conn.rollback()

    def commit_and_close(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
