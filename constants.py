SQL_STATEMENTS = {
    'connections': """
    SHOW max_connections;
    """,
    'customer': '''
        SELECT
          c.customer_id, c.last_name, c.first_name, c.email,
          a.address, a.address_id
        FROM customer c
        JOIN address a
          ON c.address_id=a.address_id
        ''',
    'payment': '''
        SELECT
          p.staff_id, p.rental_id, p.customer_id
        FROM payment p
          WHERE p.customer_id=''',
    'inventory_section': '''
        SELECT
          i.film_id, i.inventory_id
        FROM inventory i
          WHERE i.inventory_id=''',
    'film1_section': '''
        SELECT
          f.title, f.description, f.rental_duration, i.inventory_id
        FROM inventory i
        JOIN film f
          ON f.film_id=''',
    'film_section_3': '''
        SELECT
          r.customer_id, r.inventory_id
        FROM customer c
        JOIN rental r
          ON r.customer_id=''',
    'film_section_1': '''
        SELECT
          f.title, f.description, f.rental_duration, r.customer_id
        FROM customer c
        JOIN rental r
          ON r.customer_id=''',
    'film_section_2': '''
        JOIN inventory i
          ON i.inventory_id=r.inventory_id
        JOIN film f
          ON f.film_id=i.film_id
        ''',
    'store_section': '''
        SELECT
          s.store_id, s.manager_staff_id
        FROM customer c
        JOIN store s
         ON s.address_id='''
}
