from random import randint

from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler):

    # Copied from super(), modified to remove NULL values
    def as_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        result = ['INSERT INTO %s' % qn(opts.db_table)]
        fields = self.query.fields or [opts.pk]

        self.return_id = False

        if self.query.fields:
            value_rows = [
                [self.prepare_value(field, self.pre_save_val(field, obj)) for field in fields]
                for obj in self.query.objs
            ]
        else:
            # An empty object.
            value_rows = [[self.connection.ops.pk_default_value()] for _ in self.query.objs]
            fields = [None]

        glob_params_list = []
        unique_ids = []
        for row in value_rows:
            params_list = []
            if opts.auto_field:
                random_id = randint(0, 9223372036854775807)
                unique_ids.append(random_id)
                params_list.append({
                    "field": opts.auto_field,
                    "value": random_id,
                })

            for i in range(len(fields)):
                if row[i] is None:
                    continue

                params_list.append({
                    "field": fields[i],
                    "value": row[i]
                })
            glob_params_list.append(params_list)

        if unique_ids:
            self.connection.set_last_id(opts.db_table, unique_ids)

        # No NULLs
        fields = [e["field"] for e in glob_params_list[0]] # take first one
        value_rows = [
            [e["value"] for e in row] for row in glob_params_list
        ]

        result.append('(%s)' % ', '.join(qn(f.column) for f in fields))
        placeholder_rows, param_rows = self.assemble_as_sql(fields, value_rows)
        return [
            (" ".join(result + ["VALUES (%s)" % ", ".join(p)]), vals)
            for p, vals in zip(placeholder_rows, param_rows)
        ]


class SQLDeleteCompiler(compiler.SQLDeleteCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler):
    pass
