from sqlalchemy import Table, Column, String

from ckan.model import Session

import ckan.lib.jobs as jobs

from ckanext.publicamundi.model import Base


class IdentifyStatus:
    NOT_PUBLISHED = "not_published"
    REJECTED = "rejected"
    PUBLISHED = "published"


class ResourceTypes:
    VECTOR = "vector"
    RASTER = "raster"


class TaskNotReady(Exception):
    pass


class TaskFailed(Exception):
    pass


class ResourceIdentify(Base):
    __tablename__ = 'resource_identification'
    resource_id = Column('resource_id', String(64), primary_key=True)
    celery_task_id = Column('celery_task_id', String(64))
    status = Column('status', String(64))
    resource_type = Column('resource_type', String(64))

    def __init__(
            self,
            celery_task_id,
            resource_id,
            resource_type,
            status=IdentifyStatus.NOT_PUBLISHED):

        self.celery_task_id = celery_task_id
        self.resource_id = resource_id
        self.status = status
        self.resource_type = resource_type

    def get_celery_task_result(self):
        job = jobs.job_from_id(self.celery_task_id)
        result = job.result

        if result:
            return result
        else:
            raise TaskNotReady
        #TODO - how to detect failure...?
        #elif result.state == _celery.states.FAILURE:
        #    raise TaskFailed()

    def get_resource_type(self):
        return self.resource_type

    def get_identify_status(self):
        return self.status

    def set_identify_status(self, new_status):
        self.status = new_status

    def set_celery_task_id(self, new_task_id):
        self.celery_task_id = new_task_id
