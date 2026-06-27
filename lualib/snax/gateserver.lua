local skynet = require "skynet"
local netpack = require "skynet.netpack"
local socketdriver = require "skynet.socketdriver"

local gateserver = {}

local socket	-- listen socket
local queue		-- message queue
local maxclient	-- max client
local client_number = 0
local CMD = setmetatable({}, { __gc = function() netpack.clear(queue) end })
local nodelay = false

local connection = {}
-- true : connected
-- nil : closed
-- false : close read

function gateserver.openclient(fd)
	if connection[fd] then
		socketdriver.start(fd)
	end
end

function gateserver.closeclient(fd)
	local c = connection[fd]
	if c ~= nil then
		connection[fd] = nil
		socketdriver.close(fd)
	end
end

function gateserver.start(handler)
	assert(handler.message)
	assert(handler.connect)

	local listen_context = {}

	-- 该函数为 gateserver 提供 "open" 操作的实现，用于开启监听套接字并初始化大厅服务监听。
	function CMD.open(source, conf)
		-- 确保当前没有已有的套接字在监听（不能重复开启）
		assert(not socket)
		-- 读取监听地址和端口，默认监听 0.0.0.0（所有网卡）
		local address = conf.address or "0.0.0.0"
		local port = conf.port
		-- 读取最大客户端连接数，默认1024
		maxclient = conf.maxclient or 1024
		-- 是否开启 nodelay（禁用Nagle算法）
		nodelay = conf.nodelay

		-- 打印监听信息
		skynet.error("Listen on", address, port)
		
		-- 创建监听套接字
		socket = socketdriver.listen(address, port, conf.backlog)
		-- 记录当前协程和监听fd
		listen_context.co = coroutine.running()
		listen_context.fd = socket
		-- 阻塞当前协程，等待唤醒（socket 建设完成后会 resume）
		skynet.wait(listen_context.co)
		-- listener resume后，会把真正绑定的 address/port 放到 listen_context
		conf.address = listen_context.addr
		conf.port = listen_context.port
		-- 清空 listen_context 防止后续误用
		listen_context = nil
		-- 开始监听
		socketdriver.start(socket)
		-- 如果有 handler.open，回调业务层 open
		if handler.open then
			return handler.open(source, conf)
		end
	end

	function CMD.close()
		assert(socket)
		socketdriver.close(socket)
	end

	local MSG = {}

	local function dispatch_msg(fd, msg, sz)
		if connection[fd] then
			handler.message(fd, msg, sz)
		else
			skynet.error(string.format("Drop message from fd (%d) : %s", fd, netpack.tostring(msg,sz)))
		end
	end

	MSG.data = dispatch_msg

	local function dispatch_queue()
		local fd, msg, sz = netpack.pop(queue)
		if fd then
			-- may dispatch even the handler.message blocked
			-- If the handler.message never block, the queue should be empty, so only fork once and then exit.
			skynet.fork(dispatch_queue)
			dispatch_msg(fd, msg, sz)

			for fd, msg, sz in netpack.pop, queue do
				dispatch_msg(fd, msg, sz)
			end
		end
	end

	MSG.more = dispatch_queue

	function MSG.open(fd, msg)
		client_number = client_number + 1
		if client_number >= maxclient then
			socketdriver.shutdown(fd)
			return
		end
		if nodelay then
			socketdriver.nodelay(fd)
		end
		connection[fd] = true
		handler.connect(fd, msg)
	end

	function MSG.close(fd)
		if fd ~= socket then
			client_number = client_number - 1
			if connection[fd] then
				connection[fd] = false	-- close read
			end
			if handler.disconnect then
				handler.disconnect(fd)
			end
		else
			socket = nil
		end
	end

	function MSG.error(fd, msg)
		if fd == socket then
			skynet.error("gateserver accept error:",msg)
		else
			socketdriver.shutdown(fd)
			if handler.error then
				handler.error(fd, msg)
			end
		end
	end

	function MSG.warning(fd, size)
		if handler.warning then
			handler.warning(fd, size)
		end
	end

	function MSG.init(id, addr, port)
		if listen_context then
			local co = listen_context.co
			if co then
				assert(id == listen_context.fd)
				listen_context.addr = addr
				listen_context.port = port
				skynet.wakeup(co)
				listen_context.co = nil
			end
		end
	end

	skynet.register_protocol {
		name = "socket",
		id = skynet.PTYPE_SOCKET,	-- PTYPE_SOCKET = 6
		unpack = function ( msg, sz )
			return netpack.filter( queue, msg, sz)
		end,
		dispatch = function (_, _, q, type, ...)
			queue = q
			if type then
				MSG[type](...)
			end
		end
	}

	local function init()
		skynet.dispatch("lua", function (_, address, cmd, ...)
			local f = CMD[cmd]
			if f then
				skynet.ret(skynet.pack(f(address, ...)))
			else
				skynet.ret(skynet.pack(handler.command(cmd, address, ...)))
			end
		end)
	end

	if handler.embed then
		init()
	else
		skynet.start(init)
	end
end

return gateserver
