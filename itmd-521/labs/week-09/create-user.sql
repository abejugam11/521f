-- Granting privileges to the user 'root' with the password 'root' on the 'employees' database

-- Create user 'root' with password 'root'
CREATE USER 'root'@'localhost' IDENTIFIED BY 'root';

-- Grant SELECT, INSERT, CREATE, UPDATE, DELETE, DROP privileges on all tables in the 'employees' database to the user 'root'
GRANT SELECT, INSERT, CREATE, UPDATE, DELETE, DROP ON employees.* TO 'root'@'localhost';

-- Flush privileges to apply changes
FLUSH PRIVILEGES;