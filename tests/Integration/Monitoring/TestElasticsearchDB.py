
s.aggs.bucket('2','terms',field="Site", size=0, order={'1': 'desc'}).metric("1",'cardinality',field='Site')

repr(s.aggs)
"AggsProxy(aggs={'2': Terms(aggs={'1': Cardinality(field='Site')}, field='Site', order={'1': 'desc'}, size=0)})"
>>>

query = {
    'query': {
        'match_all': {}
    },
    'aggs': {
        '2': {
            'terms': {
                'field': 'Site',
                'order': {
                    '1': 'desc'
                },
                'size': 0
            },
            'aggs': {
                '1': {
                    'cardinality': {
                        'field': 'Site'
                    }
                }
            }
        }
    }
}