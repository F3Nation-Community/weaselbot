CREATE TABLE `DB_SCHEMA`.`achievements_list` (
  `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `description` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `verb` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL NOT NULL,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `DB_SCHEMA`.`achievements_awarded` (
  `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `achievement_id` int NOT NULL,
  `pax_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `date_awarded` date NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_achievement_id
    FOREIGN KEY (achievement_id) 
    REFERENCES achievements_list(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- code to populate initial achievements table for a new region
-- automatic achievements
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('The Priest', 'Post for 25 Qsource lessons', 'posting for 25 Qsource lessons', 'the_priest');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('The Monk', 'Post at 4 QSources in a month', 'posting at 4 Qsources in a month', 'the_monk');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Leader of Men', 'Q at 4 beatdowns in a month', 'Qing at 4 beatdowns in a month', 'leader_of_men');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('The Boss', 'Q at 6 beatdowns in a month', 'Qing at 6 beatdowns in a month', 'the_boss');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Be the Hammer, Not the Nail', 'Q at 6 beatdowns in a week', 'Qing at 6 beatdowns in a week', 'be_the_hammer_not_the_nail');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Cadre', 'Q at 7 different AOs in a month', 'Qing at 7 different AOs in a month', 'cadre');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('El Presidente', 'Q at 20 beatdowns in a year', 'Qing at 20 beatdowns in a year', 'el_presidente');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('El Quatro', 'Post at 25 beatdowns in a year', 'posting at 25 beatdowns in a year', 'el_quatro');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Golden Boy', 'Post at 50 beatdowns in a year', 'posting at 50 beatdowns in a year', 'golden_boy');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Centurion', 'Post at 100 beatdowns in a year', 'posting at 100 beatdowns in a year', 'centurion');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Karate Kid', 'Post at 150 beatdowns in a year', 'posting at 150 beatdowns in a year', 'karate_kid');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Crazy Person', 'Post at 200 beatdowns in a year', 'posting at 200 beatdowns in a year', 'crazy_person');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('6 pack', 'Post at 6 beatdowns in a week', 'posting at 6 beatdowns in a week', '6_pack');
INSERT INTO DB_SCHEMA.achievements_list (name, description, verb, code) VALUES('Holding Down the Fort', 'Post 50 times at an AO', 'posting 50 times at an AO', 'holding_down_the_fort');

-- manual achievements... I took out most of ours but here are some examples
-- regions could add their own manual achievements by adding to this table
-- INSERT INTO f3stcharles.achievements_list (name, description, verb, code) VALUES('You ain''t Cheatin'', you ain’t Tryin''', 'Complete a GrowRuck', 'completing a GrowRuck', 'you_aint_cheatin_you_aint_tryin');
-- INSERT INTO f3stcharles.achievements_list (name, description, verb, code) VALUES('Fall Down, Get up, Together', 'Complete MABA (3100 burpees)', 'completing MABA (>3100 burpees)', 'fall_down_get_up_together');
-- INSERT INTO f3stcharles.achievements_list (name, description, verb, code) VALUES('Redwood Original', 'Post for an inaugural beatdown for an AO launch', 'posting at an inaurgural beatdown for an AO launch', 'redwood_original');
-- INSERT INTO f3stcharles.achievements_list (name, description, verb, code) VALUES('In This Together', 'Participate in a shieldlock', 'participating in a shieldlock', 'in_this_together');
-- INSERT INTO f3stcharles.achievements_list (name, description, verb, code) VALUES('Sleeper Hold', 'EH and VQ 2 FNGs', 'EHing and VQing 2 FNGs', 'sleeper_hold');
-- INSERT INTO f3stcharles.achievements_list (name, description, verb, code) VALUES('Leave no Man Behind', 'EH 5 FNGs', 'EHing 5 FNGs', 'leave_no_man_behind');
