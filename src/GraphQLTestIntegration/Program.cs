using System;
using Azure.Core;
using Azure.Identity;
using GraphQL.Client.Http;
using GraphQL.Client.Serializer.Newtonsoft;
using Microsoft.Identity.Client;

namespace GraphQLTestIntegration
{
    class Program
    {
        private const string ClientId = ""; // This is your Application ID from Entra App Registration
        private const string TenantId = "b05b20f7-d701-422a-8295-d38ab501738b"; // This is your Tenant ID from Entra App Registration
        private const string Authority = "https://login.microsoftonline.com/" + TenantId;
        private const string GraphQLUri =
            "https://1a69dd6d-b28c-40e0-93ca-12dee95ed751.z1a.graphql.fabric.microsoft.com/v1/workspaces/1a69dd6d-b28c-40e0-93ca-12dee95ed751/graphqlapis/f5c75c15-fca2-44e3-8e0d-c123fb17ff1b/graphql"; // This is your dev GraphQL API endpoint

        static async Task Main(string[] args)
        {
            //For when authenticating against app registration:
            /*var app = PublicClientApplicationBuilder
                .Create(ClientId)
                .WithAuthority(Authority)
                .WithRedirectUri("http://localhost:1234")
                .Build();*/

            var scopes = new[] { "https://analysis.windows.net/powerbi/api/.default" };

            try
            {
                //For when authenticating against app registration:
                //var result = await app.AcquireTokenInteractive(scopes).ExecuteAsync();

                var tokenCredential = new DefaultAzureCredential(
                    new DefaultAzureCredentialOptions
                    {
                        ExcludeVisualStudioCredential = true,
                        ExcludeVisualStudioCodeCredential = true,
                    }
                );
                var accessToken = await tokenCredential.GetTokenAsync(
                    new TokenRequestContext(scopes: scopes) { }
                );

                if (accessToken.Token != null)
                {
                    Console.WriteLine("Authentication Success");

                    var graphQLClient = new GraphQLHttpClient(
                        GraphQLUri,
                        new NewtonsoftJsonSerializer()
                    );

                    graphQLClient.HttpClient.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue(
                            "Bearer",
                            accessToken.Token
                        );

                    graphQLClient.Options.IsValidResponseToDeserialize = response =>
                        response.IsSuccessStatusCode;

                    var query = new GraphQLHttpRequest
                    {
                        Query =
                            @"query {
                                      instrument_Bond_CouponRates {
                                        items {
                                          Instrument_GainID 
                                        }
                                      }
                                    }",
                    };

                    Console.WriteLine("Querying GraphQL endpoint");

                    var graphQLResponse = await graphQLClient.SendQueryAsync<dynamic>(query);

                    Console.WriteLine(graphQLResponse.Data.ToString());
                }
            }
            catch (MsalServiceException ex)
            {
                Console.WriteLine($"Authentication Error: {ex.Message}");
            }
        }
    }
}
