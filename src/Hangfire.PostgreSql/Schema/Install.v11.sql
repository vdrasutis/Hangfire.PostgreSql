ALTER TABLE "server"
  ALTER COLUMN "id" TYPE text;

ALTER TABLE "jobqueue"
  ALTER COLUMN "queue" TYPE text;

ALTER TABLE "hash"
  DROP COLUMN "updatecount";

ALTER TABLE "job"
  DROP COLUMN "updatecount";

ALTER TABLE "jobparameter"
  DROP COLUMN "updatecount";

ALTER TABLE "jobqueue"
  DROP COLUMN "updatecount";

ALTER TABLE "list"
  DROP COLUMN "updatecount";

ALTER TABLE "server"
  DROP COLUMN "updatecount";

ALTER TABLE "set"
  DROP COLUMN "updatecount";

ALTER TABLE "state"
  DROP COLUMN "updatecount";
