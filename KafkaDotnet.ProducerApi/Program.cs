using KafkaDotnet.ProducerApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddScoped<ProducerService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGet("/event-producing", async (ProducerService producer, CancellationToken cancellationToken) =>
{
    await producer.ProduceAsync(cancellationToken);
    return "Event sent!";

}).WithOpenApi();



app.Run();
