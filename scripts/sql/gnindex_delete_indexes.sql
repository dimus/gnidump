
--
-- Name: data_sources data_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY data_sources
    DROP CONSTRAINT data_sources_pkey;


--
-- Name: name_strings name_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY name_strings
    DROP CONSTRAINT name_strings_pkey;


--
-- Name: vernacular_strings vernacular_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY vernacular_strings
    DROP CONSTRAINT vernacular_strings_pkey;


--
-- Name: canonical_name_index; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX canonical_name_index;


--
-- Name: index__cmdsid_clid; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index__cmdsid_clid;


--
-- Name: index__dsid_tid; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index__dsid_tid;


--
-- Name: index__nsid_dsid_tid; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index__nsid_dsid_tid;


--
-- Name: index__vsid; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index__vsid;


--
-- Name: index_name_string_indices_on_data_source_id; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_string_indices_on_data_source_id;


--
-- Name: index_name_string_indices_on_name_string_id; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_string_indices_on_name_string_id;


--
-- Name: index_name_strings__author_words_on_author_word; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings__author_words_on_author_word;


--
-- Name: index_name_strings__genus_on_genus; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings__genus_on_genus;


--
-- Name: index_name_strings__species_on_species; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings__species_on_species;


--
-- Name: index_name_strings__subspecies_on_subspecies; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings__subspecies_on_subspecies;


--
-- Name: index_name_strings__uninomial_on_uninomial; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings__uninomial_on_uninomial;


--
-- Name: index_name_strings__year_on_year; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings__year_on_year;


--
-- Name: index_name_strings_on_canonical_uuid; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX index_name_strings_on_canonical_uuid;


--
-- Name: name_string_indices__datasource_taxonid; Type: INDEX; Schema: public; Owner: postgres
--

DROP INDEX name_string_indices__datasource_taxonid;


--
-- Name: unique_schema_migrations; Type: INDEX; Schema: public; Owner: postgres
--

DROP  INDEX unique_schema_migrations;


--
-- PostgreSQL database dump complete
--

