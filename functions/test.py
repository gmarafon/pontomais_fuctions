from Get import Get

get_test = Get(token = '$2a$10$XJ/OR9qmHwai7rS.ncXcqulBSRLMyh3fHcnpuKvjIqmZelGpi6f9e', store_type = 'csv')
#get_test.set_token('$2a$10$XJ/OR9qmHwai7rS.ncXcqulBSRLMyh3fHcnpuKvjIqmZelGpi6f9e')
get_test.local_path = 'C:\Projetos\Pontomais\csv'

get_test.call_abonos('Abonos', '2020-01-01', '2021-07-19')
get_test.call_afastamentos('Afastamentos')
get_test.call_centro_custo('Centro_Custo')
get_test.call_cidade('Cidade')
df_co = get_test.call_colaboradores('Colaboradores', return_df = True)
employee_id = df_co['id'].to_list()
get_test.call_banco_horas('Banco_Horas', employee_id)
get_test.call_departamento('Departamento')
get_test.call_excecoes_jornada('Excecoes', '2020-01-01', '2021-07-19')
get_test.call_feriados('Feriados')
get_test.call_gestores('Gestores')
get_test.call_grupo_acesso('Grupo_Acesso')
get_test.call_unidade_negocio('Unidade_Negocio')
get_test.call_usuarios('Usuario')