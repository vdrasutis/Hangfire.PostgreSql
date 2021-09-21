ALTER TABLE lock ADD COLUMN acquirer TEXT;
ALTER TABLE server ADD COLUMN lockacquirerid TEXT;