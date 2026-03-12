import discord


class BoostPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=300)
        self.bot_select = discord.ui.Select(
            placeholder="Choose custom bot",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="Custom bot", value="custom_bot", emoji="🚀")],
        )
        self.bot_select.callback = self._on_select
        self.add_item(self.bot_select)

    async def _on_select(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success)
    async def claim(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_message("Boost reward claim received.", ephemeral=True)


BOOST_REWARDS_DESCRIPTION = (
    "**Boost Rewards**\n\n"
    "1 Boost Rewards\n"
    "- Promote your server\n"
    "- Access to use banner and /dmall\n\n"
    "2 Boost Rewards\n"
    "- 1.1 boost rewards + DM all bot source code\n"
    "- 2.1 boost rewards + custom user install bot with host\n\n"
    "**Custom bot**\n"
    "User install bots work everywhere no need to add them to a server."
)
