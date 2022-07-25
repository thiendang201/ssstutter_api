# -*- coding: utf-8 -*-
import requests
import pandas as pd
import json
import mysql.connector
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import config as account
import cloudinary
import cloudinary.uploader
import cloudinary.api


cloudinary.config( 
  cloud_name = "tdclound201", 
  api_key = "938399516944169", 
  api_secret = "nuONKiIJij4iMPUr7P1J_nxQ7qY" 
)

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def connect():
    connectDb = mysql.connector.connect(
        host=account.db_host, 
        database=account.db_name, 
        user=account.db_username, 
        password=account.db_password)
    cursor = connectDb.cursor(dictionary=True)
    return connectDb, cursor

def get_total(cateId = 0):
    query = """SELECT 
        COUNT(p.id) AS total
    FROM
        product p
            JOIN
          category cate ON p.categoryId = cate.id
        AND (cate.parentsId = {} OR p.categoryId = {})""".format(cateId, cateId)
    return get_data(query)[0]["total"]

class Filter(BaseModel):
    cateId: int
    start: int
    colors: list
    sizes: list
    sort: str
    price: list
    limit: int

@app.post("/api/products/filter")
def filter_products(filter: Filter):
    sort_param = "- price" if filter.sort == "desc" else "price"
    max_query = " And price <= " + str(filter.price[1]) if filter.price[1] != -1 else ""
    min_query = " price >= " + str(filter.price[0]) if filter.price[0] != -1 else ""
    color_query = " AND v.colorId in ({})".format(', '.join([str(c) for c in filter.colors])) if len(filter.colors) > 0 else ""
    size_query = " LEFT JOIN size sz ON v.id = sz.variantId AND sz.size in ({})".format(', '.join([str(s) for s in filter.sizes])) if len(filter.sizes) > 0 else ""
    
    query = """SELECT 
        p.id,
        p.name,
        price,        
        p.img as img_url,
        sum(s.quantity) as qty,
        vc.color,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice
    FROM
        product p
            JOIN
        variation v ON p.id = v.productId """ + color_query + size_query +"""
            JOIN 
        size s on v.id = s.variantId
            LEFT JOIN
        (select vr.productId, COUNT(vr.id) as color from variation vr group by vr.productId) as vc
        ON vc.productId = p.id
            LEFT JOIN
        productsales ps ON p.id = ps.productid
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
            AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
            JOIN
		category cate ON p.categoryId = cate.id
      AND (cate.parentsId = {} OR p.categoryId = {})
 	WHERE """.format(filter.cateId, filter.cateId) + min_query + max_query + """
    GROUP BY p.id , name , price , discount , salePrice
    ORDER BY {},-p.id """.format(sort_param)

    return {
        'total': len(get_data(query)),
        'products': get_data(query + "LIMIT {}, {}".format(filter.start, filter.limit))
        }


@app.get("/api/product/detail")
def get_product(productId = 0):
    query = """SELECT 
        p.id,
        p.name,
        p.img as img_url,
        price,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice,
        description,
        categoryId
    FROM
        product p
    		JOIN 
    	variation v ON v.productId = p.id
            LEFT JOIN
        productsales ps ON p.id = ps.productid
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
            AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
    WHERE
        p.id = {}""".format(productId)
    res = get_data(query)
    rs = res[0] if len(res) > 0 else {}
    # variantsId = rs['variants'].split(',')
    
    query = """
        SELECT 
            v.id, v.thumbnail, c.name, SUM(s.quantity) AS qty
        FROM
            variation v
                JOIN
            color c ON c.id = v.colorId
                JOIN
            size s ON s.variantId = v.id
        WHERE
            v.productId = {}
        GROUP BY v.id
    """.format(productId)
    rs['variants'] = get_data(query)
    
    for v in rs['variants']:
       query = "select size, quantity from size where variantId = {}".format(v['id'])
       v['sizes'] = get_data(query)
       
       query = "select id, url from image where variantId = {}".format(v['id'])
       v['images'] = get_data(query)
    return rs
    

@app.get("/api/products/new_products")
def get_new_product(limit = 10, cateId = 0):
    query = """SELECT 
        p.id,
        p.name,
        price, 
        sum(s.quantity) as qty,
        p.img as img_url,
        vc.color,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice
    FROM
        product p
            JOIN
        variation v ON p.id = v.productId
            JOIN 
        size s on v.id = s.variantId
            LEFT JOIN
        (select vr.productId, COUNT(vr.id) as color from variation vr group by vr.productId) as vc
        ON vc.productId = p.id
            LEFT JOIN
        productsales ps ON p.id = ps.productid
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
            AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
            JOIN
		category cate ON p.categoryId = cate.id
      AND (cate.parentsId = {} OR p.categoryId = {})
	
    GROUP BY p.id , name , price , discount , salePrice
    ORDER BY -p.id LIMIT {}""".format(cateId, cateId, limit)
    return get_data(query)

@app.get("/api/products/weekly_best")
def get_weekly_best_product(limit = 8, cateId = 0):
    query = """SELECT 
        p.id,
        p.name,
        price,
        sum(s.quantity) as qty,
        p.img AS img_url,
        vc.color,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice
    FROM
        product p
            JOIN
        variation v ON p.id = v.productId
            JOIN 
        size s on v.id = s.variantId
            LEFT JOIN
        productsales ps ON p.id = ps.productid
            LEFT JOIN
        (select vr.productId, COUNT(vr.id) as color from variation vr group by vr.productId) as vc
        ON vc.productId = p.id
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
            AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
            JOIN
        category cate ON (p.categoryId = cate.id
            AND  cate.parentsId = {})
            OR p.categoryId = {}
            JOIN
        (SELECT 
            productId, SUM(quantity) qty
        FROM
            detailreceipt dr
        JOIN receipt rc ON rc.id = dr.receiptId
            AND rc.timeOrder >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        GROUP BY productId
        ORDER BY - SUM(quantity)
        LIMIT {}) h ON h.productId = p.id
    GROUP BY p.id , name , price , discount , salePrice
    ORDER BY - h.qty""".format(cateId, cateId, limit)

    # return query
    return get_data(query)

@app.get("/api/products/search")
def search_products(searchStr: str,limit: int):
    query = """SELECT 
        p.id,
        p.name,
        price,        
        sum(s.quantity) as qty,
        p.img as img_url,
        vc.color,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice
    FROM
        product p
            JOIN
        variation v ON p.id = v.productId
            JOIN 
        size s on v.id = s.variantId
            LEFT JOIN
        (select vr.productId, COUNT(vr.id) as color from variation vr group by vr.productId) as vc
        ON vc.productId = p.id
            LEFT JOIN
        productsales ps ON p.id = ps.productid
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
            AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
        WHERE p.name like '%"""+ searchStr +"""%'
    GROUP BY p.id , name , price , discount , salePrice
    ORDER BY -p.id"""
    if(searchStr == ""):
        query += """ LIMIT """  + str(limit)
    return get_data(query)

def get_sale(): 
    query = """SELECT id, name
    FROM salespromotion
    WHERE visible = 1 AND CURRENT_TIMESTAMP() BETWEEN timeStart AND timeEnd"""
    return get_data(query)

@app.get("/api/category/")
def get_category():
    query = """SELECT 
        id, name, text, img
    FROM
        category
    WHERE
        visible = 1 and id in (select parentsId from category)"""
    df = pd.DataFrame(get_data(query))
    df['type'] = 'category'
    return df.to_dict('records')


@app.get("/api/category/detail")
def get_category_detail(cateId = 0):
    query = """SELECT 
        id, name, text
    FROM
        category
    WHERE id = """ + str(cateId)
    res = get_data(query)
    if(len(res) == 0):
        return {}
    return res[0]

@app.get("/api/sizes")
def get_sizes():
    query = """SELECT distinct size FROM size"""
    return get_data(query)

@app.get("/api/colors")
def get_colors():
    query = """SELECT * FROM color"""
    return get_data(query)

@app.get("/api/max-price")
def get_max_price():
    query = """select max(price) as maxPrice from product"""
    return get_data(query)[0]["maxPrice"]

@app.get("/api/productsCollection")
def get_productsCollection(collectionId, start = -1):
    strLimit = "LIMIT {}, 8".format(start) if start != -1 else ""
    query = """SELECT 
        p.id,
        p.name,
        price,        
        sum(s.quantity) as qty,
        p.img as img_url,
        vc.color,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice
    FROM
		productcollection pc
			JOIN 
        product p on pc.productId = p.id
            JOIN
        variation v ON p.id = v.productId
            JOIN 
        size s on v.id = s.variantId
            LEFT JOIN
        (select vr.productId, COUNT(vr.id) as color from variation vr group by vr.productId) as vc
        ON vc.productId = p.id
            LEFT JOIN
        productsales ps ON p.id = ps.productid
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
        AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
		WHERE pc.collectionId = {}
    GROUP BY p.id , name , price , discount , salePrice
    ORDER BY -p.id {}""".format(collectionId, strLimit)
    return get_data(query)

@app.get("/api/collection")
def get_collection(collectionId, start):
    query = """SELECT 
        id, name, mobileBanner, pcBanner
    FROM
        collection
    WHERE
        visible = 1 and id = {}""".format(collectionId)
    rs = get_data(query)
    collection = rs[0] if len(rs) > 0 else {}
    collection["products"] = get_productsCollection(collectionId, start)
    collection["total"] = len(get_productsCollection(collectionId))
    
    return collection
    

def productsSale(salesId, size: int = -1, cateId: int = -1, start: int = -1):
    strLimit = " LIMIT {}, 8".format(start) if start != -1 else ""
    strSize = " AND size = {}".format(size) if size != -1 else ""
    strCate = """JOIN
      category cate ON p.categoryId = cate.id
    AND (cate.parentsId = {} OR p.categoryId = {})""".format(cateId, cateId) if cateId != -1 else ""
    query = """SELECT 
        p.id,
        p.name,
        price,        
        sum(s.quantity) as qty,
        p.img as img_url,
        vc.color,
        IFNULL(discount, 0) AS discount,
        IFNULL((100 - discount) * (price / 100), 0) AS salePrice
    FROM
		productsales ps
			JOIN 
        product p on ps.productId = p.id
            JOIN
        variation v ON p.id = v.productId
            JOIN 
        size s on v.id = s.variantId {}
            LEFT JOIN
        (select vr.productId, COUNT(vr.id) as color from variation vr group by vr.productId) as vc
        ON vc.productId = p.id
            LEFT JOIN
        salespromotion sp ON ps.salesid = sp.id
        AND CURRENT_TIMESTAMP() BETWEEN sp.timeStart AND sp.timeEnd
            {}
		WHERE ps.salesid = {}
    GROUP BY p.id , name , price , discount , salePrice
    ORDER BY -p.id 
    {}""".format(strSize, strCate, salesId, strLimit)
    return get_data(query) 
    
@app.get("/api/productsSales")
def get_productsSale(salesId, size: int = -1, cateId: int = -1, start: int = -1):
    rs = {}
    rs["products"] = productsSale(salesId, size, cateId, start)
    rs["total"] = len(productsSale(salesId, size, cateId))
    return rs

@app.get("/api/sales")
def get_sales(salesId, start):
    query = """SELECT 
        id, name, mobileBanner, pcBanner
    FROM
        salespromotion
    WHERE
        visible = 1
            AND CURRENT_TIMESTAMP() BETWEEN timeStart AND timeEnd 
            AND id = {}""".format(salesId)
    rs = get_data(query)
    sales = rs[0] if len(rs) > 0 else {}    
    return sales

@app.get("/api/category/children")
def get_subcate(parentsId):
    query = """SELECT 
        id, name
    FROM
        category
    WHERE
        parentsId = {}"""
        
    rs = get_data(query.format(parentsId))
    if(len(rs)==0):
        query2 = "select parentsId from category where id = {}".format(parentsId)
        rs = get_data(query2)
        parentsId = rs[0]['parentsId'] if len(rs) > 0 else 0
        rs = get_data(query.format(parentsId))
    
    df = pd.DataFrame(rs)
    df['type'] = 'category'
    return df.to_dict('records')

def get_collections():
    query = """SELECT 
        id, name
    FROM
        collection
    WHERE
        visible = 1"""
    return get_data(query)

@app.get("/api/menu")
def get_menu():
    saleList = [{'id': i['id'], 'name': i['name'], 'type': 'sale'} for i in get_sale()]
    cateList  = [{'id': i['id'], 'name': i['name'], 'type': i['type'], 'children': get_subcate(i['id'])} for i in get_category()]
    colectionList = [{'id': i['id'], 'name': i['name'], 'type': 'collection'} for i in get_collections()]
    saleList.extend(cateList)
    saleList.extend(colectionList)
    return saleList

def get_sale_banner():
    query = """SELECT 
        id, name, mobileBanner, pcBanner
    FROM
        salespromotion
    WHERE
        visible = 1
            AND CURRENT_TIMESTAMP() BETWEEN timeStart AND timeEnd"""
    df = pd.DataFrame(get_data(query))
    df['type'] = 'sale'
    return df.to_dict('records')
            
def get_collection_banner():
    query = """SELECT 
        id, name, mobileBanner, pcBanner
    FROM
        collection
    WHERE
        visible = 1"""
    df = pd.DataFrame(get_data(query))
    df['type'] = 'collection'
    return df.to_dict('records')

@app.get("/api/banner")
def get_banner():
    bannerList = get_sale_banner()
    bannerList.extend(get_collection_banner())
    return bannerList

def upLoadImg(url):
    res = cloudinary.uploader.upload(url)
    return res['url']

def get_data(query):
    connectDb, cursor = connect()
    cursor.execute(query)
    rs = cursor.fetchall()  
    connectDb.close()
    return rs

def getRecord(tableName, columnName, key):
   
    if(type(key) == str):
        query = "select * from {} where {} = '{}'".format(tableName, columnName, key)
    else:
        query = "select * from {} where {} = {}".format(tableName, columnName, key)
    rs = get_data(query)
    return rs

def insert(tableName: str, data: dict):
    connectDb, cursor = connect()
    keys = data.keys()
    col = ', '.join(keys)
    row = ', '.join(['%s'] * len(keys))
    insert = "insert  into {} ({}) values ({})".format(tableName, col, row)
    row_data = list(data.values())
    cursor.execute(insert, row_data)
    connectDb.commit()
    connectDb.close()
    return cursor.lastrowid

def insert_list_data(table_name: str, data):
    connectDb, cursor = connect()
    keys = data.keys()
    col = ', '.join(keys)
    row = ', '.join(['%s'] * len(keys))
    insert = "insert  into {} ({}) values ({})".format(table_name, col, row)
    values = [tuple(row.values) for i,row in data.iterrows()]
    cursor.executemany(insert, values)
    connectDb.commit()
    connectDb.close()
    return data.to_dict('records')
