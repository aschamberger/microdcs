SFC_RECIPE_DATASCHEMA = (
    "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/"
)
"""URI identifying the SFC recipe schema. Used as the ``dataschema`` value on
``ISA95WorkMasterDataTypeExt`` to indicate that the Work Master's ``data``
payload is an SFC recipe. The SFC engine dispatches on this URI to select
the recipe interpreter."""
