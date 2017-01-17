import hashlib
import random
import time

from google.cloud import bigtable


users = ['kukudrulu', 'batman', 'superman', 'ironman', 'spiderman']
messg = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. #{}'


def deep_to_dict(obj):
    d = obj.to_dict()
    return {k: v[0].value for k, v in d.items()}


def generate_messages(quantity=500):

    for i in range(quantity):
        tagger = users[random.randint(0, len(users)-1)]
        tagged_candidates = [u for u in users if u != tagger]
        tagged = tagged_candidates[random.randint(0, len(tagged_candidates)-1)]
        yield [tagged, tagger, messg.format(tagged)]


def main(project_id, instance_id, table_id):
    # Connection to Bigtable
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    # Create table & column family
    print('Creating the {} table.'.format(table_id))
    table = instance.table(table_id)

    users_column_family_id = 'users'
    uf = table.column_family(users_column_family_id)
    message_column_family_id = 'post'
    mf = table.column_family(message_column_family_id)

    table.create(column_families=(mf, uf))

    # Populate table
    for tagged, tagger, msg in generate_messages():
        row_key = hashlib.md5(tagged).hexdigest()[:8] + \
                  hashlib.md5(tagger).hexdigest()[:8] + \
                  str(int(time.time() * 100))
        row = table.row(row_key)
        print(row_key, tagged, tagger, msg)
        row.set_cell(users_column_family_id, 'tagged', tagged)
        row.set_cell(users_column_family_id, 'tagger', tagger)
        row.set_cell(message_column_family_id, 'msg', msg)
        row.commit()

    # Search
    row = table.read_row(row_key)
    print('{}'.format(deep_to_dict(row)))

    print('Scanning for Superman:')
    rows = table.read_rows('84d95', '84d97')
    rows.consume_all()

    for row_key, row in rows.rows.items():
        users_data = row.cells[users_column_family_id]
        print('\t{} -> {}'.format(users_data['tagger'][0].value, users_data['tagged'][0].value))
        # print('\t {}'.format(deep_to_dict(row)))
    print("Tot.: {}".format(len(rows.rows.keys())))

    # clean
    print('Deleting the {} table.'.format(table_id))
    table.delete()


if __name__ == '__main__':
    main('mystic-gradient-121905', 'test-bt', 'test-bt')
