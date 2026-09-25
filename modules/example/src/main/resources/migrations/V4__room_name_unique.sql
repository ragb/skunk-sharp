-- Room names are unique within a building, so the room-sync endpoint can match incoming rooms by name.
ALTER TABLE rooms ADD CONSTRAINT rooms_building_name_uk UNIQUE (building_id, name);
