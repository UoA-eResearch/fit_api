-- phpMyAdmin SQL Dump
-- version 4.5.4.1deb2ubuntu2
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Jan 17, 2019 at 03:44 AM
-- Server version: 5.7.18-0ubuntu0.16.04.1
-- PHP Version: 7.0.18-0ubuntu0.16.04.1

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `fit`
--
CREATE DATABASE IF NOT EXISTS `fit` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `fit`;

-- --------------------------------------------------------

--
-- Table structure for table `activity`
--

CREATE TABLE IF NOT EXISTS `activity` (
  `username` varchar(255) NOT NULL,
  `day` varchar(255) NOT NULL,
  `activity_type` int(11) NOT NULL,
  `length_ms` int(11) NOT NULL,
  `n_segments` int(11) NOT NULL,
  `lastUpdated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`username`,`day`,`activity_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `activity_goals`
--

CREATE TABLE IF NOT EXISTS `activity_goals` (
  `username` varchar(255) NOT NULL,
  `minutes` int(11) NOT NULL,
  `lastModified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `activity_types`
--

CREATE TABLE IF NOT EXISTS `activity_types` (
  `name` varchar(255) NOT NULL,
  `id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Truncate table before insert `activity_types`
--

TRUNCATE TABLE `activity_types`;
--
-- Dumping data for table `activity_types`
--

INSERT INTO `activity_types` (`name`, `id`) VALUES
('In vehicle*', 0),
('Biking*', 1),
('On foot*', 2),
('Still (not moving)*', 3),
('Unknown (unable to detect activity)*', 4),
('Tilting (sudden device gravity change)*', 5),
('Walking*', 7),
('Running*', 8),
('Aerobics', 9),
('Badminton', 10),
('Baseball', 11),
('Basketball', 12),
('Biathlon', 13),
('Handbiking', 14),
('Mountain biking', 15),
('Road biking', 16),
('Spinning', 17),
('Stationary biking', 18),
('Utility biking', 19),
('Boxing', 20),
('Calisthenics', 21),
('Circuit training', 22),
('Cricket', 23),
('Dancing', 24),
('Elliptical', 25),
('Fencing', 26),
('Football (American)', 27),
('Football (Australian)', 28),
('Football (Soccer)', 29),
('Frisbee', 30),
('Gardening', 31),
('Golf', 32),
('Gymnastics', 33),
('Handball', 34),
('Hiking', 35),
('Hockey', 36),
('Horseback riding', 37),
('Housework', 38),
('Jumping rope', 39),
('Kayaking', 40),
('Kettlebell training', 41),
('Kickboxing', 42),
('Kitesurfing', 43),
('Martial arts', 44),
('Meditation', 45),
('Mixed martial arts', 46),
('P90X exercises', 47),
('Paragliding', 48),
('Pilates', 49),
('Polo', 50),
('Racquetball', 51),
('Rock climbing', 52),
('Rowing', 53),
('Rowing machine', 54),
('Rugby', 55),
('Jogging', 56),
('Running on sand', 57),
('Running (treadmill)', 58),
('Sailing', 59),
('Scuba diving', 60),
('Skateboarding', 61),
('Skating', 62),
('Cross skating', 63),
('Inline skating (rollerblading)', 64),
('Skiing', 65),
('Back-country skiing', 66),
('Cross-country skiing', 67),
('Downhill skiing', 68),
('Kite skiing', 69),
('Roller skiing', 70),
('Sledding', 71),
('Sleeping', 72),
('Snowboarding', 73),
('Snowmobile', 74),
('Snowshoeing', 75),
('Squash', 76),
('Stair climbing', 77),
('Stair-climbing machine', 78),
('Stand-up paddleboarding', 79),
('Strength training', 80),
('Surfing', 81),
('Swimming', 82),
('Swimming (swimming pool)', 83),
('Swimming (open water)', 84),
('Table tennis (ping pong)', 85),
('Team sports', 86),
('Tennis', 87),
('Treadmill (walking or running)', 88),
('Volleyball', 89),
('Volleyball (beach)', 90),
('Volleyball (indoor)', 91),
('Wakeboarding', 92),
('Walking (fitness)', 93),
('Nording walking', 94),
('Walking (treadmill)', 95),
('Waterpolo', 96),
('Weightlifting', 97),
('Wheelchair', 98),
('Windsurfing', 99),
('Yoga', 100),
('Zumba', 101),
('Diving', 102),
('Ergometer', 103),
('Ice skating', 104),
('Indoor skating', 105),
('Curling', 106),
('Other (unclassified fitness activity)', 108),
('Light sleep', 109),
('Deep sleep', 110),
('REM sleep', 111),
('Awake (during sleep cycle)', 112),
('Crossfit', 113),
('HIIT', 114),
('Interval Training', 115),
('Walking (stroller)', 116),
('Elevator', 117),
('Escalator', 118),
('Archery', 119),
('Softball', 120);

-- --------------------------------------------------------

--
-- Table structure for table `google_fit`
--

CREATE TABLE IF NOT EXISTS `google_fit` (
  `username` varchar(255) NOT NULL,
  `google_id` varchar(255) NOT NULL,
  `full_name` varchar(255) NOT NULL,
  `gender` varchar(255) DEFAULT NULL,
  `image_url` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `refresh_token` varchar(255) NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`google_id`),
  KEY `google_id` (`google_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `steps`
--

CREATE TABLE IF NOT EXISTS `steps` (
  `username` varchar(255) NOT NULL,
  `day` varchar(255) NOT NULL,
  `steps` int(11) NOT NULL,
  `lastUpdated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`username`,`day`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
