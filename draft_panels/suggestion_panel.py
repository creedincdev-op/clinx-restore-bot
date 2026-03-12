import discord


class SuggestionModal(discord.ui.Modal, title="Suggestion Form"):
    title_input = discord.ui.TextInput(label="Title", placeholder="Feature title", max_length=100)
    details_input = discord.ui.TextInput(
        label="Suggestion",
        placeholder="Describe your idea...",
        style=discord.TextStyle.paragraph,
        max_length=1500,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message("Suggestion received.", ephemeral=True)


class SuggestionPanelView(discord.ui.View):
    @discord.ui.button(label="Suggestion", emoji="💡", style=discord.ButtonStyle.primary)
    async def open_suggestion(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(SuggestionModal())


SUGGESTION_PANEL_DESCRIPTION = (
    "Submit your idea to help improve the bot!\n\n"
    "Click the Suggestion button below to open the submission form."
)
