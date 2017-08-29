
--
-- Name: data_sources data_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY data_sources
    ADD CONSTRAINT data_sources_pkey PRIMARY KEY (id);


--
-- Name: name_strings name_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY name_strings
    ADD CONSTRAINT name_strings_pkey PRIMARY KEY (id);


--
-- Name: vernacular_strings vernacular_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY vernacular_strings
    ADD CONSTRAINT vernacular_strings_pkey PRIMARY KEY (id);


--
-- Name: canonical_name_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX canonical_name_index ON name_strings USING btree (canonical text_pattern_ops);


--
-- Name: index__cmdsid_clid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index__cmdsid_clid ON cross_maps USING btree (cm_data_source_id, cm_local_id);


--
-- Name: index__dsid_tid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index__dsid_tid ON vernacular_string_indices USING btree (data_source_id, taxon_id);


--
-- Name: index__nsid_dsid_tid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index__nsid_dsid_tid ON cross_maps USING btree (data_source_id, name_string_id, taxon_id);


--
-- Name: index__vsid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index__vsid ON vernacular_string_indices USING btree (vernacular_string_id);


--
-- Name: index_name_string_indices_on_data_source_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_string_indices_on_data_source_id ON name_string_indices USING btree (data_source_id);


--
-- Name: index_name_string_indices_on_name_string_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_string_indices_on_name_string_id ON name_string_indices USING btree (name_string_id);


--
-- Name: index_name_strings__author_words_on_author_word; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings__author_words_on_author_word ON name_strings__author_words USING btree (author_word);


--
-- Name: index_name_strings__genus_on_genus; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings__genus_on_genus ON name_strings__genus USING btree (genus);


--
-- Name: index_name_strings__species_on_species; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings__species_on_species ON name_strings__species USING btree (species);


--
-- Name: index_name_strings__subspecies_on_subspecies; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings__subspecies_on_subspecies ON name_strings__subspecies USING btree (subspecies);


--
-- Name: index_name_strings__uninomial_on_uninomial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings__uninomial_on_uninomial ON name_strings__uninomial USING btree (uninomial);


--
-- Name: index_name_strings__year_on_year; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings__year_on_year ON name_strings__year USING btree (year);


--
-- Name: index_name_strings_on_canonical_uuid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_name_strings_on_canonical_uuid ON name_strings USING btree (canonical_uuid);


--
-- Name: name_string_indices__datasource_taxonid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX name_string_indices__datasource_taxonid ON name_string_indices USING btree (data_source_id, taxon_id);


--
-- Name: unique_schema_migrations; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX unique_schema_migrations ON schema_migrations USING btree (version);


--
-- PostgreSQL database dump complete
--

