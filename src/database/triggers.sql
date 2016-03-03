CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


CREATE TRIGGER action_update_timestamp BEFORE UPDATE ON action FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER dataset_update_timestamp BEFORE UPDATE ON dataset FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER datastore_update_timestamp BEFORE UPDATE ON datastore FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER engine_update_timestamp BEFORE UPDATE ON engine FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER event_update_timestamp BEFORE UPDATE ON event FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER message_update_timestamp BEFORE UPDATE ON message FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER subscription_update_timestamp BEFORE UPDATE ON subscription FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER subscription_element_update_timestamp BEFORE UPDATE ON subscription_element FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER trigger_update_timestamp BEFORE UPDATE ON trigger FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER workflow_update_timestamp BEFORE UPDATE ON workflow FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER workflow_instance_update_timestamp BEFORE UPDATE ON workflow_instance FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
