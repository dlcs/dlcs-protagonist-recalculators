-- Create user for CustomerStorage recalculator script
CREATE USER customerstoragerecalc_user WITH ENCRYPTED PASSWORD 'xxx';
GRANT SELECT, INSERT, UPDATE ON "CustomerStorage" TO customerstoragerecalc_user;
GRANT SELECT ON "ImageStorage" TO customerstoragerecalc_user;

-- Create user for CustomerStorage recalculator script
CREATE USER entitycounterrecalc_user WITH ENCRYPTED PASSWORD 'xxx';
GRANT SELECT, INSERT, UPDATE ON "EntityCounters" TO entitycounterrecalc_user;
GRANT SELECT ON "Images" TO entitycounterrecalc_user;