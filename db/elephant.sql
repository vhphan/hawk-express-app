
-- create a table named events to track messages between servers

CREATE TABLE events (
    id serial PRIMARY KEY,
    created_at timestamp NOT NULL DEFAULT now(),
    from_server varchar(30) NOT NULL,
    to_server varchar(30) NOT NULL,
    json_message jsonb NOT NULL,
    text_message text NOT NULL
);


-- create trigger event when new row is added to events table

CREATE OR REPLACE FUNCTION notify_event() RETURNS trigger AS $$
DECLARE
    payload json;
BEGIN   
    payload = json_build_object(
        'id', NEW.id,
        'created_at', NEW.created_at,
        'from_server', NEW.from_server,
        'to_server', NEW.to_server,
        'json_message', NEW.json_message,
        'text_message', NEW.text_message
    );
    PERFORM pg_notify('events', payload::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;



-- trigger notify_event when new row is added to events table

CREATE TRIGGER notify_event
AFTER INSERT ON events
FOR EACH ROW EXECUTE PROCEDURE notify_event();
