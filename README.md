# CS525-F25-G02

## Illinois Institute of Technology

### *CS 525: Advanced Database Organization — Fall 2025*

## 项目简介

本项目为 **IIT（伊利诺伊理工学院）2025 年秋季学期《CS525 高级数据库系统组织（Advanced Database Organization）》课程设计项目**。
整个课程分为四次编程作业，逐步实现了一个简化版数据库管理系统的核心模块，包括文件管理、缓存管理、记录管理以及索引管理（B+ 树）。

每次作业的成果均保存在独立的文件夹中：

* `assign1/` —— **Storage Manager**：实现文件的页级管理，支持页的创建、读取、写入与删除。
* `assign2/` —— **Buffer Manager**：在内存中实现缓冲池机制，支持 FIFO / LRU 等页面替换算法。
* `assign3/` —— **Record Manager**：实现关系型表的记录存取、扫描、更新与条件查询功能。
* `assign4/` —— **B+ Tree Index Manager**：实现基于页文件与缓存管理器的 B+ 树索引模块，支持插入、删除、查找与遍历。

该项目的目标是掌握数据库系统的核心机制 **页式存储、缓存策略、记录结构、索引查找** 。


### **Team Members**

* **Hongyi Jiang** (A20506636) — [jiangxiaobai1142@gmail.com](mailto:jiangxiaobai1142@gmail.com)
* **Naicheng Wei** (A20278475) — [lwei3@ghawk.illinoistech.edu](mailto:lwei3@ghawk.illinoistech.edu)


## 课程来源

> Illinois Institute of Technology
> Department of Computer Science
> *CS525: Advanced Database Organization — Fall 2025*

## 文件结构

```
CS525-F25-G02/
│
├── assign1/      # Storage Manager
├── assign2/      # Buffer Manager
├── assign3/      # Record Manager
├── assign4/      # B+ Tree Index Manager
└── README.md
```


## 功能与实现说明

| 模块              | 核心功能         | 关键点                     |
| --------------- | ------------ | ----------------------- |
| Storage Manager | 文件页的读写与管理    | 实现页缓冲、文件偏移与页大小控制        |
| Buffer Manager  | 缓冲池 + 页面替换算法 | FIFO / LRU 策略、固定计数、脏页写回 |
| Record Manager  | 关系型记录管理      | Schema 定义、扫描与条件表达式解析    |
| B+ Tree Index   | 索引结构实现       | 支持插入、删除、叶/非叶节点分裂与合并     |

## 致谢与声明

本项目中的部分框架代码与接口定义来自课程提供的模板，仅用于教学与学习目的。
所有代码与文档版权归课程作者与项目成员共同所有，请勿商用。

## Description of this project

### **CS525 - Advanced Database Organization (Fall 2025, IIT)**

This repository contains the full coursework for **CS 525: Advanced Database Organization** at **Illinois Institute of Technology (IIT)**, Fall 2025 semester.
The course focuses on building a mini relational database engine through four progressive programming assignments.

### **Assignments Overview**

* `assign1/` — **Storage Manager**: Implements page-based file I/O operations.
* `assign2/` — **Buffer Manager**: Adds memory buffer pool and page replacement strategies (FIFO / LRU).
* `assign3/` — **Record Manager**: Handles relational tuples, schema, and scan operations with conditional queries.
* `assign4/` — **B+ Tree Index Manager**: Implements a disk-based B+ Tree index integrated with the buffer manager.



### **Acknowledgement**

This project is part of IIT’s **CS525 - Advanced Database Organization** course.
Starter code and specifications were provided by the course instructors and are used here strictly for educational purposes.

### **Directory Structure**

```
CS525-F25-G02/
├── assign1/      # Storage Manager
├── assign2/      # Buffer Manager
├── assign3/      # Record Manager
├── assign4/      # B+ Tree Index Manager
└── README.md
```

