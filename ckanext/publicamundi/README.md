#Patch in things from the bigger publicamundi
RUN mkdir -p /usr/lib/ckan/default/src/ckanext-vectorstorer/ckanext/publicamundi/model
WORKDIR /usr/lib/ckan/default/src/ckanext-vectorstorer/ckanext/publicamundi
RUN wget https://raw.githubusercontent.com/PublicaMundi/ckanext-publicamundi/bdd4d6d6c8473a180127b419df9547bed6c89a23/ckanext/publicamundi/__init__.py
WORKDIR /usr/lib/ckan/default/src/ckanext-vectorstorer/ckanext/publicamundi/model
RUN wget https://raw.githubusercontent.com/PublicaMundi/ckanext-publicamundi/bdd4d6d6c8473a180127b419df9547bed6c89a23/ckanext/publicamundi/model/__init__.py
RUN wget https://raw.githubusercontent.com/PublicaMundi/ckanext-publicamundi/bdd4d6d6c8473a180127b419df9547bed6c89a23/ckanext/publicamundi/model/csw_record.py
RUN wget https://raw.githubusercontent.com/derilinx/ckanext-publicamundi/c1590e89d7c50509ea7cb5dee2189614e10a1ca2/ckanext/publicamundi/model/resource_identify.py

