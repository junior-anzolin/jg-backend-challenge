# Architecture

Este documento registra as principais decisões técnicas do desafio, seus motivos e os trade-offs considerados. As decisões poderão ser revisadas conforme a implementação e os requisitos forem evoluindo.

## 1. Estratégia de versionamento e fluxo de desenvolvimento

Neste desafio técnico, os commits são realizados diretamente na branch `main` para simplificar o fluxo de trabalho individual e manter um histórico incremental das etapas de implementação.

Em um ambiente de produção, adotaria um fluxo baseado em branches de funcionalidade e Pull Requests, com a branch `main` protegida contra alterações diretas.

O processo incluiria:

- **Branches de funcionalidade:** cada tarefa ou conjunto coeso de alterações seria desenvolvido isoladamente.
- **Pull Requests:** as alterações seriam submetidas à revisão antes de serem incorporadas à branch principal.
- **Verificações automatizadas:** testes, validações estáticas e outras etapas de CI seriam executados antes da aprovação.
- **Proteção da branch principal:** regras de aprovação, verificações obrigatórias e restrições a pushes diretos e *force pushes*, conforme a política da equipe.

Essa diferença reflete uma adaptação ao contexto do desafio técnico. Em um ambiente colaborativo, o fluxo de integração deve favorecer a revisão de código, a qualidade contínua e a segurança da branch principal.

## 2. Biblioteca para aritmética monetária: `decimal.js`

Foi escolhido o `decimal.js` considerando a maturidade do projeto, os sinais de adoção e participação comunitária no GitHub e a integração com TypeScript.

Tanto `decimal.js` quanto `big.js` são bibliotecas maduras e atendem às necessidades de aritmética decimal deste desafio. O `decimal.js` apresenta maior engajamento visível no GitHub e inclui suas próprias declarações de tipos TypeScript, enquanto o `big.js` utiliza um pacote externo para essa integração.

Em um projeto NestJS com TypeScript strict, manter a implementação e seus contratos de tipos distribuídos pelo mesmo pacote reduz uma dependência externa e simplifica a configuração do projeto.

A escolha não implica que `decimal.js` seja matematicamente superior a `big.js` para as operações atuais. Ambas são opções adequadas; a decisão prioriza a integração com TypeScript e os indicadores de adoção considerados.

## 3. Política de retry da Outbox

A publicação de mensagens da Outbox utiliza **exponential backoff**, aumentando progressivamente o intervalo entre tentativas para evitar chamadas excessivas quando o serviço de mensageria estiver temporariamente indisponível.

A política inicial adotada é:

- **Intervalo inicial:** 1 segundo.
- **Fator de crescimento:** 2 — o intervalo dobra a cada tentativa.
- **Intervalo máximo:** 5 minutos.
- **Agendamento:** cada chamada a `scheduleRetry(now)` incrementa o contador `attempts` e atualiza `nextAttemptAt`, usando o horário recebido pelo método.

| Tentativas registradas | Espera até a próxima tentativa |
| ---: | ---: |
| 1 | 1 segundo |
| 2 | 2 segundos |
| 3 | 4 segundos |
| 4 | 8 segundos |
| 5 | 16 segundos |
| Posteriores | Crescimento exponencial, limitado a 5 minutos |

O objetivo é reduzir a pressão sobre o serviço durante falhas temporárias, permitindo que a publicação seja retomada sem intervenção manual imediata.

O estado da mensagem, incluindo o contador de tentativas e o próximo horário de publicação, deverá ser persistido para que o processamento possa continuar após reinicializações da aplicação.

**Limitações atuais:** a política define um intervalo máximo, mas ainda não estabelece um limite para a quantidade total de tentativas. Também não utiliza *jitter* (variação aleatória dos intervalos). Essas decisões poderão ser revisadas conforme os requisitos operacionais forem definidos.

O README exige backoff exponencial, mas não determina os intervalos utilizados. Os valores acima são decisões adotadas nesta implementação.
