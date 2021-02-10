--Cria a tabela temporária que receberá a importação do arquivo. Nessa tabela não haverão regras, ela é apenas intermediária
create table public."dados_brutos" (
	cpf varchar(255),
	private varchar(255),
	incompleto varchar(255),
	ultima_compra varchar(255),
	ticket_medio varchar(255),
	ticket_ultima_compra varchar(255),
	loja_mais_frequente varchar(255),
	loja_ultima_compra varchar(255)
);

--Cria a tabela que receberá os dados higienizados do arquivo de importação. Essa tabela que deverá ser consumida
create table public."dados_limpos" (
	cpf varchar(20),
	private integer,
	incompleto integer,
	ultima_compra date,
	ticket_medio decimal(10, 2),
	ticket_ultima_compra decimal(10, 2),
	loja_mais_frequente varchar(25),
	loja_ultima_compra varchar(25),
	cpf_valido boolean,
	cnpj_compras_frequentes_valido boolean,
	cnpj_ult_compra_valido boolean,
	constraint pk_cpf primary key (cpf)
);

-- Função para validar o CPF
create or replace function checkCpf(in in_cpf varchar(11)) returns boolean as $$
declare
  x real;
  y real;
  soma integer;
  dig1 integer;
  dig2 integer;
  len integer;
  contloop integer;
  val_in_cpf varchar(11);
begin
  if char_length(in_cpf) != 11 then
    return false;
  end if;
  
  x := 0;
  soma := 0;
  dig1 := 0;
  dig2 := 0;
  contloop := 0;
  val_in_cpf := $1;  
  len := char_length(val_in_cpf);
  x := len - 1;
 
  contloop := 1;
  while contloop <= (len - 2) loop
	  y := cast(substring(val_in_cpf from contloop for 1) as numeric);
	  soma := soma + (y * x);
	  x := x - 1;
	  contloop := contloop + 1;
  end loop;

  dig1 := 11 - cast((soma % 11) as integer);
  
  if (dig1 = 10) then 
 	  dig1 := 0; 
  end if;

  if (dig1 = 11) then 
    dig1 := 0;
  end if;

  x := 11; 
  soma := 0;
  
  contloop := 1;
  while contloop <= (len -1) loop
	  soma := soma + cast((substring(val_in_cpf from contloop for 1)) as real) * x;
	  x := x - 1;
	  contloop := contloop + 1;
  end loop;
 
  dig2 := 11 - cast((soma % 11) as integer);
 
  if (dig2 = 10) then 
 	  dig2 := 0; 
  end if;
 
  if (dig2 = 11) then 
    dig2 := 0; 
  end if;
 
  if ((dig1 || '' || dig2) = substring(val_in_cpf from len-1 for 2)) then
    return true;
  else
    return false;
  end if;
end;
$$ language plpgsql;


-- Função para validar o CNPJ
create or replace function checkCnpj(in in_cnpj varchar(14))
returns boolean as
$$
declare    
  v_cnpj_quebrado smallint[];    
  c_posicao_dv1 constant smallint default 13;
  v_arranjo_dv1 smallint[12] default array[5,4,3,2,9,8,7,6,5,4,3,2];
  v_soma_dv1 smallint default 0;
  v_resto_dv1 double precision default 0;    
  c_posicao_dv2 constant smallint default 14;
  v_arranjo_dv2 smallint[13] default array[6,5,4,3,2,9,8,7,6,5,4,3,2];
  v_soma_dv2 smallint default 0;
  v_resto_dv2 double precision default 0;    
begin
  v_cnpj_quebrado := regexp_split_to_array(regexp_replace(in_cnpj, '[^0-9]', '', 'g'), '');        
  
  for t in 1..12 loop
    v_soma_dv1 := v_soma_dv1 + (v_cnpj_quebrado[t] * v_arranjo_dv1[t]);
  end loop;
  
  v_resto_dv1 := ((10 * v_soma_dv1) % 11) % 10;
       
  if (v_resto_dv1 != v_cnpj_quebrado[c_posicao_dv1]) then
    return false;
  end if;
    
      
  for t in 1..13 loop
    v_soma_dv2 := v_soma_dv2 + (v_cnpj_quebrado[t] * v_arranjo_dv2[t]);
  end loop;
    
  v_resto_dv2 := ((10 * v_soma_dv2) % 11) % 10;
    
  return (v_resto_dv2 = v_cnpj_quebrado[c_posicao_dv2]);  
end;
$$ language plpgsql;


-- Função invocada pela trigger que chama as funções de validação
create or replace function callCheckCpfAndCnpj() returns trigger as 
$trgCheckCpfAndCnpj$
declare
  x_valid boolean;
begin
  if (not new.cpf is null) then
    select checkCpf(new.cpf) into x_valid;
    new.cpf_valido := x_valid;
  end if;
 
  if (not new.loja_mais_frequente is null) then
    select checkCnpj(new.loja_mais_frequente) into x_valid;
	  new.cnpj_compras_frequentes_valido := x_valid;
	else
	  new.cnpj_compras_frequentes_valido := false;
  end if;

  if (not new.loja_ultima_compra is null) then
	  select checkCnpj(new.loja_ultima_compra) into x_valid;
	  new.cnpj_ult_compra_valido := x_valid;
	else
	  new.cnpj_ult_compra_valido := false;
  end if;
	
  return new;
end;
$trgCheckCpfAndCnpj$ language plpgsql;


-- Trigger disparada durante as inserções na tabela de dados limpos para validar os documentos (CPF e CNPJ)
create trigger trgCheckCpfAndCnpj before insert or update of cpf, loja_mais_frequente, loja_ultima_compra on dados_limpos for each row execute procedure callCheckCpfAndCnpj();