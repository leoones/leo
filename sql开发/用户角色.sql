--- 创建用户
CREATE USER [IF NOT EXISTS | OR REPLACE] name
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
    [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
    [DEFAULT ROLE {role[,...] | NONE}]
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [ACCOUNT {LOCK | UNLOCK}]
    
--- 更新用户   
 ALTER USER [IF EXISTS] name
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
    [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
    [DEFAULT ROLE {role[,...] | NONE | ALL}]
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [UNSET {varname [,...] | ALL}]
    [ACCOUNT {LOCK | UNLOCK}]
    
--- 删除用户
DROP USER [IF EXISTS] name [,name,...]

--- 创建角色
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name [,name,...]

--- 角色赋权
GRANT {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    TO user_or_role [, user_or_role ...]
    [WITH GRANT OPTION]
    
GRANT
    role [, role ...]
    TO user_or_role [, user_or_role...]
    [WITH ADMIN OPTION]
    
---  角色权限回收
 REVOKE [GRANT OPTION FOR]
    {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    FROM user_or_role [, user_or_role ...]
    
 REVOKE [ADMIN OPTION FOR]
    role [, role ...]
    FROM user_or_role [, user_or_role...]

-- 查看角色权限
SHOW GRANTS FOR user_or_role


--- 角色赋予用户    
SET DEFAULT ROLE
    {role [,role...] | NONE | ALL}
    TO user [, user ] ...
    
    
    
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
    
    
ALTER SETTINGS PROFILE [IF EXISTS] name
    [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
    [UNSET {varname [,...] | ALL}]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
    
 SHOW CREATE SETTINGS PROFILE name


CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
    {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
    [KEYED BY USERNAME | KEYED BY IP | NOT KEYED] [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
    
    
ALTER QUOTA [IF EXIST] name
    {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
    [KEYED BY USERNAME | KEYED BY IP | NOT KEYED] [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
    
SHOW CREATE QUOTA name
 
DROP QUOTA [IF EXISTS] name [,name...]
 
 
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] name
    ON {database.table | table}
    USING condition 
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
    
    
ALTER [ROW] POLICY [IF EXISTS] name
    ON {database.table | table}
    USING condition 
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
    
    
 SHOW CREATE [ROW] POLICY name
 
 
 DROP [ROW] POLICY [IF EXISTS] name [,name...]
