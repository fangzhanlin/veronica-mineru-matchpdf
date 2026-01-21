#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据源抽象层 (v3.0)

提供统一的数据源接口，支持 CSV 和 MongoDB 等多种数据源。
支持字段映射，适配不同数据源的字段命名差异。

Author: GitHub Copilot
Date: 2025-01-20
"""

import csv
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


@dataclass
class FieldMapping:
    """
    字段映射配置
    
    用于在不同数据源之间统一字段名称。
    """
    title: str = 'Title'      # 标题字段
    doi: str = 'DOI'          # DOI 字段
    uuid: str = ''            # UUID 字段（用于 MongoDB）
    
    def to_dict(self) -> Dict[str, str]:
        """返回字段映射字典"""
        return {
            'title': self.title,
            'doi': self.doi,
            'uuid': self.uuid,
        }


# 预定义的字段映射
CSV_FIELD_MAPPING = FieldMapping(title='Title', doi='DOI', uuid='')
MONGODB_FIELD_MAPPING = FieldMapping(title='label', doi='doi', uuid='uuid')


@dataclass
class Record:
    """
    文献记录数据类
    
    统一的记录格式，无论来自 CSV 还是 MongoDB
    """
    data: Dict[str, Any]
    source_id: Optional[str] = None  # 记录来源标识（如文件名、集合名）
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取字段值"""
        return self.data.get(key, default)
    
    def __getitem__(self, key: str) -> Any:
        return self.data[key]
    
    def __contains__(self, key: str) -> bool:
        return key in self.data
    
    def copy(self) -> 'Record':
        """创建副本"""
        return Record(data=self.data.copy(), source_id=self.source_id)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return self.data.copy()


@dataclass
class DataSourceResult:
    """
    数据源查询结果
    """
    records: List[Record]
    headers: List[str]
    source_name: str
    field_mapping: FieldMapping = field(default_factory=lambda: CSV_FIELD_MAPPING)
    total_count: int = 0
    
    def __post_init__(self):
        if self.total_count == 0:
            self.total_count = len(self.records)


class DataSource(ABC):
    """
    数据源抽象基类
    
    定义了数据源的标准接口，所有具体数据源都需要实现这些方法。
    """
    
    def __init__(
        self,
        field_mapping: Optional[FieldMapping] = None,
        logger: Optional[logging.Logger] = None
    ):
        self.field_mapping = field_mapping or CSV_FIELD_MAPPING
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def connect(self) -> bool:
        """连接到数据源"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """断开数据源连接"""
        pass
    
    @abstractmethod
    def get_records(
        self,
        source_identifier: str = "",
        query: Optional[Dict[str, Any]] = None
    ) -> DataSourceResult:
        """获取记录"""
        pass
    
    @abstractmethod
    def get_available_sources(self) -> List[str]:
        """获取所有可用的数据源标识"""
        pass
    
    @property
    @abstractmethod
    def source_type(self) -> str:
        """返回数据源类型标识"""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class CSVDataSource(DataSource):
    """
    CSV 文件数据源
    """
    
    def __init__(
        self,
        csv_dir: Optional[Path] = None,
        csv_file: Optional[Path] = None,
        file_pattern: str = "*.csv",
        encoding: str = 'utf-8-sig',
        field_mapping: Optional[FieldMapping] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        初始化 CSV 数据源
        
        Args:
            csv_dir: CSV 文件所在目录（与 csv_file 二选一）
            csv_file: 单个 CSV 文件路径（与 csv_dir 二选一）
            file_pattern: 文件匹配模式
            encoding: 文件编码
            field_mapping: 字段映射配置
            logger: 日志记录器
        """
        super().__init__(field_mapping or CSV_FIELD_MAPPING, logger)
        self.csv_dir = Path(csv_dir) if csv_dir else None
        self.csv_file = Path(csv_file) if csv_file else None
        self.file_pattern = file_pattern
        self.encoding = encoding
        self._connected = False
        self._csv_files: List[Path] = []
    
    @property
    def source_type(self) -> str:
        return "csv"
    
    def connect(self) -> bool:
        """扫描并缓存 CSV 文件列表"""
        if self.csv_file:
            # 单文件模式
            if not self.csv_file.exists():
                self.logger.error(f"CSV 文件不存在: {self.csv_file}")
                return False
            self._csv_files = [self.csv_file]
        elif self.csv_dir:
            # 目录模式
            if not self.csv_dir.exists():
                self.logger.error(f"CSV 目录不存在: {self.csv_dir}")
                return False
            self._csv_files = sorted(self.csv_dir.glob(self.file_pattern))
        else:
            self.logger.error("必须指定 csv_dir 或 csv_file")
            return False
        
        self._connected = True
        self.logger.info(f"CSV 数据源已连接，找到 {len(self._csv_files)} 个文件")
        return True
    
    def disconnect(self) -> None:
        """清理资源"""
        self._csv_files = []
        self._connected = False
        self.logger.debug("CSV 数据源已断开")
    
    def get_records(
        self,
        source_identifier: str = "",
        query: Optional[Dict[str, Any]] = None
    ) -> DataSourceResult:
        """
        读取 CSV 文件的记录
        
        Args:
            source_identifier: CSV 文件路径，为空时使用第一个文件
            query: 可选的过滤条件
            
        Returns:
            DataSourceResult
        """
        # 确定要读取的文件
        if source_identifier:
            csv_path = Path(source_identifier)
            if not csv_path.is_absolute() and self.csv_dir:
                csv_path = self.csv_dir / csv_path
        elif self._csv_files:
            csv_path = self._csv_files[0]
        else:
            raise ValueError("没有可用的 CSV 文件")
        
        records = []
        headers = []
        
        try:
            with open(csv_path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                
                for row in reader:
                    if query:
                        match = all(row.get(k) == v for k, v in query.items())
                        if not match:
                            continue
                    
                    records.append(Record(
                        data=dict(row),
                        source_id=csv_path.stem
                    ))
            
            self.logger.debug(f"从 {csv_path.name} 读取 {len(records)} 条记录")
            
        except Exception as e:
            self.logger.error(f"读取 CSV 文件 {csv_path} 时出错: {e}")
            raise
        
        return DataSourceResult(
            records=records,
            headers=headers,
            source_name=csv_path.stem,
            field_mapping=self.field_mapping
        )
    
    def get_available_sources(self) -> List[str]:
        """获取所有 CSV 文件路径"""
        return [str(f) for f in self._csv_files]


class MongoDBDataSource(DataSource):
    """
    MongoDB 数据源
    
    从 MongoDB 读取文献记录数据。
    支持字段映射：doi -> DOI, label -> Title, uuid -> UUID
    """
    
    def __init__(
        self,
        connection_string: str,
        database: str,
        collection: str = "",
        field_mapping: Optional[FieldMapping] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        初始化 MongoDB 数据源
        
        Args:
            connection_string: MongoDB 连接字符串
            database: 数据库名称
            collection: 集合名称（可选，也可在 get_records 时指定）
            field_mapping: 字段映射配置，默认使用 MongoDB 映射
            logger: 日志记录器
        """
        super().__init__(field_mapping or MONGODB_FIELD_MAPPING, logger)
        self.connection_string = connection_string
        self.database_name = database
        self.collection_name = collection
        self._client = None
        self._db = None
    
    @property
    def source_type(self) -> str:
        return "mongodb"
    
    def connect(self) -> bool:
        """连接到 MongoDB"""
        try:
            from pymongo import MongoClient
            
            self._client = MongoClient(self.connection_string)
            self._db = self._client[self.database_name]
            
            # 测试连接
            self._client.admin.command('ping')
            
            self.logger.info(f"MongoDB 已连接: {self.database_name}")
            return True
            
        except ImportError:
            self.logger.error("pymongo 未安装。请运行: uv add pymongo")
            return False
        except Exception as e:
            self.logger.error(f"MongoDB 连接失败: {e}")
            return False
    
    def disconnect(self) -> None:
        """断开 MongoDB 连接"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self.logger.debug("MongoDB 已断开")
    
    def get_records(
        self,
        source_identifier: str = "",
        query: Optional[Dict[str, Any]] = None
    ) -> DataSourceResult:
        """
        从 MongoDB 集合获取记录
        
        Args:
            source_identifier: 集合名称，为空时使用初始化时的 collection
            query: MongoDB 查询条件
            
        Returns:
            DataSourceResult
        """
        if self._db is None:
            raise RuntimeError("MongoDB 未连接")
        
        collection_name = source_identifier or self.collection_name
        if not collection_name:
            raise ValueError("未指定集合名称")
        
        collection = self._db[collection_name]
        mongo_query = query or {}
        
        records = []
        headers = set()
        
        cursor = collection.find(mongo_query)
        
        for doc in cursor:
            doc_dict = dict(doc)
            # 将 _id 转为字符串
            if '_id' in doc_dict:
                doc_dict['_id'] = str(doc_dict['_id'])
            
            headers.update(doc_dict.keys())
            records.append(Record(
                data=doc_dict,
                source_id=collection_name
            ))
        
        self.logger.debug(f"从集合 {collection_name} 读取 {len(records)} 条记录")
        
        return DataSourceResult(
            records=records,
            headers=sorted(headers),
            source_name=collection_name,
            field_mapping=self.field_mapping
        )
    
    def get_available_sources(self) -> List[str]:
        """获取所有集合名称"""
        if self._db is None:
            return []
        return self._db.list_collection_names()


def create_data_source(
    source_type: str,
    **kwargs
) -> DataSource:
    """
    数据源工厂函数
    
    Args:
        source_type: 数据源类型 ('csv' 或 'mongodb')
        **kwargs: 传递给具体数据源的参数
        
    Returns:
        DataSource 实例
    """
    if source_type == 'csv':
        return CSVDataSource(**kwargs)
    elif source_type == 'mongodb':
        return MongoDBDataSource(**kwargs)
    else:
        raise ValueError(f"不支持的数据源类型: {source_type}")
