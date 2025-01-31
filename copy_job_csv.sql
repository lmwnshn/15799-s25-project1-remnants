-- TODO(WAN): check if the bug exists in core DuckDB and send a PR?

-- COPY aka_name FROM '/home/kapi/git/join-order-benchmark-copy/extracted/aka_name.csv';
-- COPY aka_title FROM '/home/kapi/git/join-order-benchmark-copy/extracted/aka_title.csv';
-- COPY cast_info FROM '/home/kapi/git/join-order-benchmark-copy/extracted/cast_info.csv';
-- COPY char_name FROM '/home/kapi/git/join-order-benchmark-copy/extracted/char_name.csv';
-- COPY comp_cast_type FROM '/home/kapi/git/join-order-benchmark-copy/extracted/comp_cast_type.csv';
-- COPY company_name FROM '/home/kapi/git/join-order-benchmark-copy/extracted/company_name.csv';
-- COPY company_type FROM '/home/kapi/git/join-order-benchmark-copy/extracted/company_type.csv';
-- COPY complete_cast FROM '/home/kapi/git/join-order-benchmark-copy/extracted/complete_cast.csv';
-- COPY info_type FROM '/home/kapi/git/join-order-benchmark-copy/extracted/info_type.csv';
-- COPY keyword FROM '/home/kapi/git/join-order-benchmark-copy/extracted/keyword.csv';
-- COPY kind_type FROM '/home/kapi/git/join-order-benchmark-copy/extracted/kind_type.csv';
-- COPY link_type FROM '/home/kapi/git/join-order-benchmark-copy/extracted/link_type.csv';
-- COPY movie_companies FROM '/home/kapi/git/join-order-benchmark-copy/extracted/movie_companies.csv';
-- COPY movie_info FROM '/home/kapi/git/join-order-benchmark-copy/extracted/movie_info.csv';
-- COPY movie_info_idx FROM '/home/kapi/git/join-order-benchmark-copy/extracted/movie_info_idx.csv';
-- COPY movie_keyword FROM '/home/kapi/git/join-order-benchmark-copy/extracted/movie_keyword.csv';
-- COPY movie_link FROM '/home/kapi/git/join-order-benchmark-copy/extracted/movie_link.csv';
-- COPY name FROM '/home/kapi/git/join-order-benchmark-copy/extracted/name.csv';
-- COPY person_info FROM '/home/kapi/git/join-order-benchmark-copy/extracted/person_info.csv';
-- COPY role_type FROM '/home/kapi/git/join-order-benchmark-copy/extracted/role_type.csv';
-- COPY title FROM '/home/kapi/git/join-order-benchmark-copy/extracted/title.csv';

INSERT INTO aka_name SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/aka_name.csv', ignore_errors=true);
INSERT INTO aka_title SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/aka_title.csv', ignore_errors=true);
INSERT INTO cast_info SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/cast_info.csv', ignore_errors=true);
INSERT INTO char_name SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/char_name.csv', ignore_errors=true);
INSERT INTO comp_cast_type SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/comp_cast_type.csv', ignore_errors=true);
INSERT INTO company_name SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/company_name.csv', ignore_errors=true);
INSERT INTO company_type SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/company_type.csv', ignore_errors=true);
INSERT INTO complete_cast SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/complete_cast.csv', ignore_errors=true);
INSERT INTO info_type SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/info_type.csv', ignore_errors=true);
INSERT INTO keyword SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/keyword.csv', ignore_errors=true);
INSERT INTO kind_type SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/kind_type.csv', ignore_errors=true);
INSERT INTO link_type SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/link_type.csv', ignore_errors=true);
INSERT INTO movie_companies SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/movie_companies.csv', ignore_errors=true);
INSERT INTO movie_info SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/movie_info.csv', ignore_errors=true);
INSERT INTO movie_info_idx SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/movie_info_idx.csv', ignore_errors=true);
INSERT INTO movie_keyword SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/movie_keyword.csv', ignore_errors=true);
INSERT INTO movie_link SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/movie_link.csv', ignore_errors=true);
INSERT INTO name SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/name.csv', ignore_errors=true);
INSERT INTO person_info SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/person_info.csv', ignore_errors=true);
INSERT INTO role_type SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/role_type.csv', ignore_errors=true);
INSERT INTO title SELECT * FROM read_csv('./join-order-benchmark-copy/imdb_data/title.csv', ignore_errors=true);