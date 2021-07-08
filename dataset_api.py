#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import Row

row = Row(350, True, "learning spark", None)

for i in range(len(row)):
    print(row[i])

