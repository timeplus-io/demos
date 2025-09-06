-- Create the demo user
CREATE USER demo IDENTIFIED BY 'demo123';

-- Grant access ONLY to demo database
GRANT ALL ON demo.* TO demo;

-- Verify the user was created
SHOW USERS;

-- Check permissions
SHOW GRANTS FOR demo;

REVOKE ALL ON system.* FROM demo;
REVOKE ALL ON default.* FROM demo;