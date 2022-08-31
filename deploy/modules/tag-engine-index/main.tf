resource "google_firestore_index" "index" {
  project    = var.project_name
  collection = var.collection_name

  dynamic "fields" {
    # The __name__ is confusing. terraform docs says that this will be
    # added automatically, but it fails to recognize it when comparing existing state to desired state and always tries to recreate all indices.
    for_each = concat(var.field_names, ["__name__"])
    content {
      field_path = fields.value
      order      = "ASCENDING"
    }
  }

}