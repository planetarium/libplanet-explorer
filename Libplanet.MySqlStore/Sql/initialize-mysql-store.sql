CREATE TABLE IF NOT EXISTS `libplanet.mysql`.`Block` (
  `index`                 INT,
  `hash`                  VARCHAR,
  `pre_evaluation_hash`   VARCHAR,
  `state_root_hash`       VARCHAR,
  `difficulty`            BIGINT,
  `total_difficulty`      BIGINT,
  `nonce`                 VARCHAR,
  `miner`                 VARCHAR,
  `previous_hash`         VARCHAR,
  `timestamp`             VARCHAR,
  `tx_hash`               VARCHAR,
  `bytes_length`          INT,
  `key`                   VARBINARY,
  `value`                 VARBINARY,
  PRIMARY KEY (`index`),
  UNIQUE INDEX `index_UNIQUE` (`index` ASC)
);

CREATE TABLE IF NOT EXISTS `libplanet.mysql`.`Transaction` (
  `id`              VARCHAR,
  `nonce`           VARCHAR,
  `signer`          VARCHAR,
  `signature`       VARCHAR,
  `timestamp`       VARCHAR,
  `public_key`      VARCHAR,
  `genesis_hash`    VARCHAR,
  `bytes_length`    VARCHAR,
  `key`             VARBINARY,
  `value`           VARBINARY
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC)
);

CREATE TABLE IF NOT EXISTS `libplanet.mysql`.`Updated_Address` (
  `address`           VARCHAR,
  `Transaction_id`    VARCHAR,
  INDEX `fk_Updated_Address_Transaction_idx` (`Transaction_id` ASC),
  PRIMARY KEY (`address`),
  CONSTRAINT `fk_Updated_Address_Transaction`
    FOREIGN KEY (`Transaction_id`)
    REFERENCES `libplanet.mysql`.`Transaction` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE TABLE IF NOT EXISTS `libplanet.mysql`.`Chain` (
  `key`     VARBINARY,
  `value`   VARBINARY,
  `cf`      VARCHAR,
  `prefix`  VARBINARY,
  PRIMARY KEY (`Key`,`Cf`)
);

CREATE TABLE IF NOT EXISTS `libplanet.mysql`.`State` (
  `key`     VARBINARY,
  `value`   VARBINARY,
  PRIMARY KEY (`key`),
    UNIQUE INDEX `key_UNIQUE` (`key` ASC)
);

CREATE TABLE IF NOT EXISTS `libplanet.mysql`.`State_Hash` (
  `key`     VARBINARY,
  `value`   VARBINARY,
  PRIMARY KEY (`key`),
    UNIQUE INDEX `key_UNIQUE` (`key` ASC)
);
