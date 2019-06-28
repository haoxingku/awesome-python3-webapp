import logging; logging.basicConfig(level=logging.INFO)
# 一次使用异步 处处使用异步
import aiomysql,asyncio

def log(sql):
    logging.info('SQL: %s'%(sql))
# 通过关键字参数**kw接受连接数据库需要的对应参数来创建连接池
async def create_pool(loop,**kw):
	'''
    创建数据库链接池
    :param loop:事件循环处理程序
    :param kw:数据库配置参数集合
    :return:无
    缺省情况下将编码设置为utf8，自动提交事务
    '''
	logging.info('create database connection pool...')
	gloabal __pool
	__pool=await aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['root'],
		password=kw['root'],
		db=kw['db'],
		charset=kw.get('charset','utf-8'),
		autocmmit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
	)
#用于输出元类中创建sql_insert语句中的占位符
def create_args_string(num):
	'''
	用来计算需要拼接多少个占位符
    :param num:
    :return:
	'''
	L=[]
	for x in range(num):
		L.append('?')
	return ','.join(L)

#用于输出元类中创建sql_insert语句中的占位符
#单独封装select，其他insert,update,delete一并封装，理由如下：
#使用Cursor对象执行insert，update，delete语句时，执行结果由rowcount返回影响的行数，就可以拿到执行结果。
#使用Cursor对象执行select语句时，通过featchall()可以拿到结果集。结果集是一个list，每个元素都是一个tuple，对应一行记录。

# 传入SQL语句，参数，大小可选
async def select(sql,args,size=None):
	log(sql,args)
	global __pool # 获取全局的连接池__pool
	async with  __pool.get() as conn: # 打开连接池
		async with conn.cursor(aiomysql.DictCursor) as cur: # 创建游标，DictCursor的作用是使查询返回的结果为字典格式
			await cur.execute(sql.replace('?','%s'),args or ()) # 执行SQL语句，将SQL语句的'?'占位符替换成MySQL的'%s'占位符
			if size: # 如果有传入size，则返回对应个数的结果，size为None则返回全部
				rs=await cur.fetchmany(size)
			else:
				rs=await cur.fetchall()
		logging.info('rows returned:%s' %len(rs))
		return rs
		
# 传入SQL语句，参数，默认自动提交事务		
async def execute(sql,args,autocmmit=True):
   '''
    Insert、Update、Delete操作的公共执行函数
    :param sql:sql语句
    :param args:sql参数
    :param autocommit:自动提交事务
    :return:
    '''
	log(sql)
	async with await __pool.get() as conn:
		if not autocmmit:# 如果autocommit为False，conn.begin()开始事务
			await conn.begin()
		try:# 无论是否自动提交事务，都执行try中的代码
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?','%s'),args)
				affected=cur.rowcount # 通过rowcount得到SQL语句影响的行数
			if not autocommit:
				await conn.commit()
		except BaseException as e:# 处理出错情况
			if not autocmmit:
				await conn.rollback()# 回滚操作
			raise
		return affected

#定义一个Field类型和其子类对应数据库中不同的类型，String、Integer、Boolean、Float、Text等。
class Field(Object):
	def __init__(self,name,colum_type,primary_key,default):
		self.name=name # 字段名
		self.colum_type=colum_type # 列类型
		self.primary_key=primary_key # 主键
		self.default=default # 默认值
		
	def _str_(self):
		return '<%s,%s:%s>'%(self.__class__.__name__,self.colum_type,self.name)
		
class StringField(Field):
		
	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):
	def __init__(self,name=None,default=False):
		super().__init__(name,'bolean',false,default)
		
class IntegerField(Field):
	def __init__(self,name=None,primary_key=False,default=0):
		super().__init__(name,'bigint',primary_key,default)
		
class FloatField(Field):
	def __init__(self,name=None,primary_key=False,default=0.0):
		super().__init__(name,'real',primary_key,default)
		
class TextField(Field):
	def __init__(self,name=None,default=None):
		super().__init__(name,'text',False,default)
	
#通过元类ModelMetaclass可以将具体的子类映射信息读取出来。
#它的元类负责分类、整理收集的数据并以此创建一些类属性(如SQL语句)供基类作为参数。	
class ModelMetaclass(type):
	# 调用__init__方法前会调用__new__方法
	def __new__(cls,name,bases,attrs): 
	# cls：当前准备创建的类的对象，name：类的名称，bases：类继承的父类集合，attrs：类的方法集合
		# 排除Model类本身:
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)
		# 获取table名称;如果未设置，tableName就是类的名字
		tableName=attrs.get('__table__',None) or name
		logging.info('found model:%s (table:%s)') %(name,tableName))
		 # 获取所有的Field和主键名:
		mappings=dict()
		fields=[]
		primarykey=None
		# key是列名，value是field的子类
		for k,v in attrs.item():
			if isinstance(v,Field):
				logging.info('found mapping:%s ==> %s' %(k,v))
				mappings[k]=v
				if v.primary_key:
					#找到主键
					if primarykey:
						raise StandardError('Duplicate primary key for field:%S' % k)
					primarykey=k
				else:
					fields.append(k)
		if not primarykey:
			raise StandardError('primary key not found.')
		# 删除类属性
		for k in mappings.keys():
			attrs.pop(k)
		# 保存除主键外的属性名为``(运算出字符串)列表形式
		escaped_fields=list(map(lambda f:'`%s`' % f,fields))
		attrs['__mappings__']=mappings#保存属性和列的映射关系
		attrs['__table__']=tableName
		attrs['__primary_key__']=primaryKey#主键属性名
		attrs['__fields__']=fields#除主键名的属性名
		#构造默认的select，insert，upate和delete语句
		# 反引号和repr()函数的功能一致
		attrs['__select__']='select `%s`,`%s` from `%s`' % (primaryKey,','.join(escaped_fields),tableName)
		attrs['__insert__']='insert into `%s` (%s,`%s`) values(%s)' %s (tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
		attrs['__update__']='update `%s` set %s where `%s`=?' %(tableName,','.join(map(lambda f:'`%s`=?'%(mappings.get(f).name or f),fields)),primaryKey)
		attrs['__delete__']='delete from `%s`=?'%(tableName,primaryKey)
		return type.__new__(cls,name,bases,attrs)

#基类负责执行操作，比如数据库的存储、读取，查找等操作；
#继承自Model的类，会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性中__table__、__mappings__等。
class Model(dict,metaclass=ModelMetaclass):
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)
		
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribut '%s'" % key)
			
	def __setattr__(self,key,value):
		self[key]=value
		
	def getValue(self,key):
		# 返回对象的属性，如果没有对应属性，则会调用__getattr__
		return getter(self,key,None)#直接调回内置函数，注意这里没有下划符,注意这里None的用处,是为了当user没有赋值数据时，返回None，调用于update
	
	def getValueOrDefault(self,key):
		value=getter(self,key,None)#第三个参数None，可以在没有返回数值时，返回None，调用于save
		if value is None:
			field=self.__mapping__[key]
			if field.default is not None:
				value=field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s:%s' %(key,str(value)))
				# 将默认值设置进行
				setattr(self,key,value)
		return value
			
	# 类方法第一个参数为cls，而实例方法第一个参数为self
	@classmethod
	async def finaAll(cls,where=None,args=None,**kw):
		'find object by where clause.'
		sql=[cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args=[]
		orderBy=kw.get('orderBy',None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit=kw.get('limit',None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit,int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit,tuple) and len(limit)==2:
				sql.append('?,?')
				# extend接受一个iterable参数
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value: %s' % str(limit))
		rs=await select(' '.join(sql),args)
		return [cls(**r) for r in rs]
		
		@classmethod
		async def findNumber(cls,selectField,where=None):
			'find number by select and where'
			# 将列名重命名为_num_
			sql=['select %s _num_ from `%s`'% (selectField,cls.__table__)]
			if where:
				sql.append('where')
				sql.append(where)
			# 限制结果数为1
			rs=await select(''.join(sql),args,1)
			if len(rs)==0:
				return None
			return rs[0]['_num_']
			
		@classmethod
		async def find(cls,pk):
			'find object by primary key.'
			rs=await select('%s where `%s`=?'%(cls.__select__,cls.__primary_key__),[pk],1)
			if len(rs)==0:
				return None
			return cls(**rs[0])
			
		async def save(self):
			# 获取所有value
			args=list(map(self.getValueOrDefault,self.__fields__))
			args.append(self.getValueOrDefault(self.__primary_key__))
			rows= await execute(self.__insert__,args)
			if rows !=1:
				logging.warn('failed to update by primary key: affected rows: %s' % rows)
		
		async def update(self):
			args=list(map(self.getValue,self.__fields__))
			args.append(self.getValue(self.__primary_key__))
			rows=await execute(self.__update__,args)
			if rows !=1:
				logging.warn('failed to update by primary key:affect rows:%s' % rows)
		
		async def remove(self):
			args=[self.getValue(self.__primary_key__)]
			rows=await execute(self.__delete__,args)
			if rows ！=1:
				logging.warn('failed to remove by primary key:affect rows:%s' % rows)
			
		