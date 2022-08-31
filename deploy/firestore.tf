# I've redone all the indexes to use a generic module (under modules/tag-engine-index)
# I've removed the dependency tree that was used to keep parallelism,
# and instead we are using the `-parallelism` flag of the terraform CLI (=4)

module "index-1" {
  source          = "./modules/tag-engine-index"
  collection_name = "static_configs"
  field_names     = ["config_type", "included_uris_hash", "template_uuid", "config_status"]
  project_name    = var.tag_engine_project
}
module "index-2" {
  source          = "./modules/tag-engine-index"
  collection_name = "dynamic_configs"
  field_names     = ["config_type", "included_uris_hash", "template_uuid", "config_status"]
  project_name    = var.tag_engine_project
}

module "index-3" {
  project_name    = var.tag_engine_project
  collection_name = "entry_configs"
  source          = "./modules/tag-engine-index"
  field_names     = ["config_type", "included_uris_hash", "template_uuid", "config_status"]
}

module "index-4" {
  source          = "./modules/tag-engine-index"
  project_name    = var.tag_engine_project
  collection_name = "glossary_configs"
  field_names     = ["config_type", "included_uris_hash", "template_uuid", "config_status"]
}

module "index-5" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "sensitive_configs"
  field_names     = ["config_type", "included_uris_hash", "template_uuid", "config_status"]
}

module "index-6" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "dynamic_configs"
  field_names     = ["config_status", "refresh_mode", "scheduling_status", "next_run"]
}

module "index-7" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "static_configs"
  field_names     = ["config_status", "refresh_mode", "scheduling_status", "next_run"]
}

module "index-8" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "entry_configs"
  field_names     = ["config_status", "refresh_mode", "scheduling_status", "next_run"]
}

module "index-9" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "glossary_configs"
  field_names     = ["config_status", "refresh_mode", "scheduling_status", "next_run"]
}


module "index-10" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "sensitive_configs"
  field_names     = ["config_status", "refresh_mode", "scheduling_status", "next_run"]
}


module "index-12" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "tasks"
  field_names     = ["job_uuid", "config_uuid", "uri"]
}

module "index-13" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "restore_configs"
  field_names     = ["group_key", "source_template_uuid", "target_template_uuid"]
}

module "index-14" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "dynamic_configs"
  field_names     = ["group_key", "template_uuid", "config_status"]
}

module "index-15" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "static_configs"
  field_names     = ["group_key", "template_uuid", "config_status"]
}

module "index-16" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "entry_configs"
  field_names     = ["group_key", "template_uuid", "config_status"]
}

module "index-17" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "glossary_configs"
  field_names     = ["group_key", "template_uuid", "config_status"]
}

module "index-18" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "sensitive_configs"
  field_names     = ["group_key", "template_uuid", "config_status"]
}

module "index-19" {
  source       = "./modules/tag-engine-index"
  project_name = var.tag_engine_project

  collection_name = "restore_configs"
  field_names     = ["group_key", "target_template_uuid", "config_status"]
}

module "index-20" {
  source          = "./modules/tag-engine-index"
  project_name    = var.tag_engine_project
  collection_name = "import_configs"
  field_names     = ["group_key", "metadata_import_location", "template_uuid", "config_status"]
}

module "index-21" {
  source          = "./modules/tag-engine-index"
  project_name    = var.tag_engine_project
  collection_name = "import_configs"
  field_names     = ["group_key", "template_uuid", "config_status"]
}

# This one index has a DESCENDING field, so we can either change the module to receive a map of fields instead of a list
# or we keep this only one as a manual entry
resource "google_firestore_index" "index-11" {
  project    = var.tag_engine_project
  collection = "logs"

  fields {
    field_path = "config_type"
    order = "ASCENDING"
  }
  fields {
    field_path = "res"
    order = "ASCENDING"
  }
  fields {
    field_path = "ts"
    order = "DESCENDING"
  }
  fields {
    field_path = "__name__"
    order = "ASCENDING"
  }
}