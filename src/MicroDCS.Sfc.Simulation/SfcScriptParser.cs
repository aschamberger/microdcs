using System.Globalization;
using System.Text.Json;

namespace MicroDCS.Sfc.Simulation;

/// <summary>
/// Text forms for tools and tests. A script is JSON:
/// <c>{"default":"ok","replies":{"action":["ok","error:jam@500","silent"]},"events":[{"atMs":1000,"typeId":"t.qa","outcome":"ok"}]}</c>.
/// A reply is <c>silent</c>, <c>error[:text][@delayMs]</c> or <c>outcome[@delayMs]</c>.
/// </summary>
public static class SfcScriptParser
{
    /// <summary>Parses a script document.</summary>
    /// <param name="json">The JSON text; empty gives the default script.</param>
    /// <returns>The script.</returns>
    /// <exception cref="FormatException">The document is not a valid script; the message names the problem.</exception>
    public static SfcScript Parse(string? json)
    {
        var script = new SfcScript();
        if (string.IsNullOrWhiteSpace(json))
        {
            return script;
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException("The script must be a JSON object.");
            }

            foreach (JsonProperty property in document.RootElement.EnumerateObject())
            {
                switch (property.Name)
                {
                    case "default":
                        script.Default = ParseReply(property.Value);
                        break;
                    case "replies":
                        ParseReplies(script, property.Value);
                        break;
                    case "events":
                        ParseEvents(script, property.Value);
                        break;
                    default:
                        throw new FormatException($"Unknown script field '{property.Name}'; use default, replies or events.");
                }
            }
        }
        catch (JsonException ex)
        {
            throw new FormatException($"The script is not valid JSON (at '{ex.Path}').", ex);
        }

        return script;
    }

    /// <summary>Builds a recipe for a dry run or an explanation: every feature is on and every outcome the recipe names is declared.</summary>
    /// <param name="recipe">The recipe JSON (UTF-8).</param>
    /// <returns>The graph or the build errors. Outcome typos cannot be found this way; a real station checks them against its processors.</returns>
    public static SfcBuildResult BuildPermissive(ReadOnlyMemory<byte> recipe) => SfcGraph.Build(recipe, Permissive);

    private static SfcCapabilities Permissive(SfcRecipe parsed)
    {
        HashSet<string> named = [.. parsed.Transitions.Select(static t => t.Condition).Where(static c => c != SfcOutcome.Default), "ok"];
        return new SfcCapabilities(
            SfcFeature.PushCommand | SfcFeature.PullEvent | SfcFeature.SimultaneousDivergence | SfcFeature.SelectionDivergence,
            parsed.Actions
                .Select(static a => a.TypeId)
                .Distinct(StringComparer.Ordinal)
                .ToDictionary(static t => t, _ => (IReadOnlySet<string>)named, StringComparer.Ordinal));
    }

    private static void ParseReplies(SfcScript script, JsonElement replies)
    {
        if (replies.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("'replies' must be an object of action name to a list of replies.");
        }

        foreach (JsonProperty action in replies.EnumerateObject())
        {
            if (action.Value.ValueKind != JsonValueKind.Array)
            {
                throw new FormatException($"The replies of action '{action.Name}' must be a list.");
            }

            script.For(action.Name, [.. action.Value.EnumerateArray().Select(ParseReply)]);
        }
    }

    private static void ParseEvents(SfcScript script, JsonElement events)
    {
        if (events.ValueKind != JsonValueKind.Array)
        {
            throw new FormatException("'events' must be a list.");
        }

        foreach (JsonElement item in events.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object
                || !item.TryGetProperty("atMs", out JsonElement at) || !at.TryGetInt64(out long atMs)
                || !item.TryGetProperty("typeId", out JsonElement type) || type.ValueKind != JsonValueKind.String)
            {
                throw new FormatException("Each event needs atMs (a number) and typeId (a string); outcome is optional.");
            }

            string outcome = item.TryGetProperty("outcome", out JsonElement o) && o.ValueKind == JsonValueKind.String ? o.GetString()! : "ok";
            script.Inject(new SfcEventReceived { At = atMs, TypeId = type.GetString()!, Outcome = outcome, Subject = OptionalText(item, "subject"), CorrelationId = OptionalText(item, "correlationId") });
        }
    }

    private static string? OptionalText(JsonElement item, string name) =>
        item.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static SfcReply ParseReply(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.String)
        {
            throw new FormatException("A reply is a string: silent, error[:text][@delayMs] or outcome[@delayMs].");
        }

        string text = element.GetString() ?? string.Empty;
        long delay = 0;
        int at = text.LastIndexOf('@');
        if (at >= 0)
        {
            if (!long.TryParse(text[(at + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out delay))
            {
                throw new FormatException($"The delay in reply '{text}' must be a whole number of milliseconds.");
            }

            text = text[..at];
        }

        if (text.Length == 0)
        {
            throw new FormatException("A reply needs an outcome name.");
        }

        if (text == "silent")
        {
            return SfcReply.Silence();
        }

        if (text == "error" || text.StartsWith("error:", StringComparison.Ordinal))
        {
            return SfcReply.Fail(text.Length > 6 ? text[6..] : "failed", delay);
        }

        return SfcReply.Ok(text, delay);
    }
}
