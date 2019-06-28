#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'url handlers'

import re,time,json,logging,hashlib,base64,asyncio

from coreweb import get,post

from models import User,Comment,Blog,next_id

@get('/')
async def index(request):
	user = await User.findAll()
	return {
		'__temolate__':'test.html',
		'users':users
	}