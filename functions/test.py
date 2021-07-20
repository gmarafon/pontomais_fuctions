from Get import Get

get_test = Get()
get_test.set_token('$2a$10$XJ/OR9qmHwai7rS.ncXcqulBSRLMyh3fHcnpuKvjIqmZelGpi6f9e')
get_test.local_path = 'C:\Projetos\Pontomais\csv'
#get_test.call_abonos('Abonos', '2020-01-01', '2021-07-19')
get_test.call_afastamentos('Afastamentos')
