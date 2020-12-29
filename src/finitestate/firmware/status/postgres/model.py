# coding: utf-8

#
# model.py
#
# Comprehensive SQLAlchemy model of the fwan_status_ database tables
#

import datetime
import enum
import os
import sqlalchemy as sa
import sys
from sqlalchemy import Column, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy_utils import create_view

# resolving parent directory imports
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

Base = declarative_base()
metadata = Base.metadata


@enum.unique
class ActiveStage(enum.Enum):
    """ Possible stages for firmware analysis, risk assessment, and various rollups """
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    ANALYSIS = enum.auto()
    RISK = enum.auto()


def utc_now():
    """ Convenience function for generating TZ accurate NOW time/date """
    return datetime.datetime.utcnow()


class ProcessStatusDetail(Base):
    __tablename__ = 'process_status_detail'
    __table_args__ = (Index('process_status_detail_idx', 'fwan_process_id', 'file_id', 'component_id', 'status'), )

    status_id = Column(UUID, primary_key=True, server_default=sa.text("uuid_generate_v4()"))
    fwan_process_id = Column(UUID)
    file_id = Column(Text)
    component_id = Column(Text)
    status = Column(Text)
    status_date = Column(DateTime(True))

    # Relationship
    error_detail = relationship("ErrorDetail", uselist=False, back_populates="process_status_detail")
    result_status = relationship("ResultStatus", uselist=False, back_populates="process_status_detail")


class ErrorDetail(Base):
    __tablename__ = 'error_detail'

    status_id = Column(ForeignKey('process_status_detail.status_id'), primary_key=True)
    details = Column(JSONB(astext_type=Text()))

    # Relationship
    process_status_detail = relationship("ProcessStatusDetail", back_populates="error_detail")


class ActiveComponent(Base):
    __tablename__ = 'active_component'
    __table_args__ = (Index('active_component_start_idx', 'active_start_date'), )

    fwan_process_id = Column(UUID, primary_key=True)
    file_id = Column(Text, primary_key=True)
    component_id = Column(Text, primary_key=True)
    active_start_date = Column(DateTime(True), nullable=False, default=utc_now)


class ExpiredComponent(Base):
    __tablename__ = 'expired_component'
    __table_args__ = (Index('expired_component_idx', 'expired_date'), )

    fwan_process_id = Column(UUID, primary_key=True)
    file_id = Column(Text, primary_key=True)
    component_id = Column(Text, primary_key=True)
    expired_date = Column(DateTime(True), nullable=False, default=utc_now)


class ActiveProcess(Base):
    __tablename__ = 'active_process'
    __table_args__ = (Index('active_process_start_idx', 'active_start_date'), )

    fwan_process_id = Column(UUID, primary_key=True)
    file_id = Column(Text, primary_key=True)
    active_stage = Column(sa.Enum(ActiveStage), nullable=False)
    active_start_date = Column(DateTime(True), nullable=False, default=utc_now)


class ResultStatus(Base):
    __tablename__ = 'result_status'

    status_id = Column(ForeignKey('process_status_detail.status_id'), primary_key=True)
    # result_code takes a string from the FSPluginOutputResult, but because SQLAlchemy
    # is having troubles resolving Enum in a Text column (from a bad migration script)
    # we're reverting this to a plain string.  Ugly.
    result_code = Column(Text, nullable=False)

    # Relationship
    process_status_detail = relationship("ProcessStatusDetail", back_populates="result_status")


class ProcessResultView(Base):
    __table__ = create_view(name='process_result_view',
                            selectable=sa.select([
                                ProcessStatusDetail.status_id, ProcessStatusDetail.fwan_process_id, ProcessStatusDetail.file_id,
                                ProcessStatusDetail.component_id, ProcessStatusDetail.status, ProcessStatusDetail.status_date,
                                ResultStatus.result_code,
                                ErrorDetail.details.label('error_details')
                            ],
                                                 from_obj=(ProcessStatusDetail.__table__.join(
                                                     ResultStatus,
                                                     ProcessStatusDetail.status_id == ResultStatus.status_id,
                                                     isouter=True).join(ErrorDetail,
                                                                        ProcessStatusDetail.status_id == ErrorDetail.status_id,
                                                                        isouter=True))),
                            metadata=Base.metadata)
