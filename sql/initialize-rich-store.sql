CREATE TABLE IF NOT EXISTS `tx_references` (
    CONSTRAINT `uid` UNIQUE (`tx_id`, `block_hash`),

    `tx_id`         BINARY(32),
    `block_hash`    BINARY(32),
    `tx_nonce`      BIGINT
);

CREATE TABLE IF NOT EXISTS `signer_references` (
    CONSTRAINT `uid` UNIQUE (`signer`, `tx_id`),

    `signer`    BINARY(20),
    `tx_id`     BINARY(32),
    `tx_nonce`  BIGINT
);

CREATE TABLE IF NOT EXISTS `updated_address_references` (
    CONSTRAINT `uid` UNIQUE (`updated_address`, `tx_id`),

    `updated_address`   BINARY(20),
    `tx_id`             BINARY(32),
    `tx_nonce`          BIGINT
);

CREATE TABLE IF NOT EXISTS `block` (
  `index`                 BIGINT,
  `hash`                  BINARY(32),
  `pre_evaluation_hash`   BINARY(32),
  `state_root_hash`       BINARY(32),
  `difficulty`            BIGINT,
  `total_difficulty`      BIGINT,
  `nonce`                 BINARY(32),
  `miner`                 BINARY(32),
  `previous_hash`         BINARY(32),
  `timestamp`             VARCHAR,
  `tx_hash`               BINARY(32),
  `bytes_length`          INT,
    PRIMARY KEY (`hash`),
    UNIQUE INDEX `hash_UNIQUE` (`hash` ASC)
);

CREATE TABLE IF NOT EXISTS `transaction` (
  `tx_id`               BINARY(32),
  `nonce`               BIGINT,
  `signer`              BINARY(20),
  `signature`           BINARY(71),
  `timestamp`           VARCHAR,
  `public_key`          VARCHAR,
  `genesis_hash`        BINARY(32),
  `bytes_length`        INT,
  PRIMARY KEY (`tx_id`),
  UNIQUE INDEX `tx_id_UNIQUE` (`tx_id` ASC)
);
